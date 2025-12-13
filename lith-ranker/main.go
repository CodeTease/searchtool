package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Cấu Hình Lith Rank ---
const (
	DampingFactor        = 0.85 // Xác suất người dùng tiếp tục click
	DefaultMaxIterations = 20   // Số vòng lặp để tính điểm
	DefaultInterval      = 60   // Phút (Mặc định tính lại mỗi tiếng)
)

// CompactGraph optimizes memory usage using parallel arrays (Struct of Arrays)
// instead of map[int64]*Page, and using int32 indices for links.
type CompactGraph struct {
	// Node data
	IDs            []int64
	Scores         []float64
	NewScores      []float64
	LinksOut       []int32
	TextLengths    []int32
	CrawledAts     []int64 // Unix timestamp
	Freshness      []float64
	Quality        []float64

	// Graph structure (Adjacency List using local indices)
	// LinksIn[i] contains the list of *indices* (not IDs) that point to node i.
	LinksIn        [][]int32

	// Map ID to Index (only used during loading, cleared after)
	idToIndex map[int64]int32
}

func NewCompactGraph(capacity int) *CompactGraph {
	return &CompactGraph{
		IDs:         make([]int64, 0, capacity),
		Scores:      make([]float64, 0, capacity),
		NewScores:   make([]float64, 0, capacity),
		LinksOut:    make([]int32, 0, capacity),
		TextLengths: make([]int32, 0, capacity),
		CrawledAts:  make([]int64, 0, capacity),
		Freshness:   make([]float64, 0, capacity),
		Quality:     make([]float64, 0, capacity),
		LinksIn:     make([][]int32, 0, capacity),
		idToIndex:   make(map[int64]int32, capacity),
	}
}

func (g *CompactGraph) AddNode(id int64, textLength int, crawledAt time.Time) {
	idx := int32(len(g.IDs))
	g.IDs = append(g.IDs, id)
	g.Scores = append(g.Scores, 0.0)
	g.NewScores = append(g.NewScores, 0.0)
	g.LinksOut = append(g.LinksOut, 0)
	g.TextLengths = append(g.TextLengths, int32(textLength))
	g.CrawledAts = append(g.CrawledAts, crawledAt.Unix())
	g.Freshness = append(g.Freshness, 0.0)
	g.Quality = append(g.Quality, 0.0)
	g.LinksIn = append(g.LinksIn, []int32{}) // Initialize empty slice
	g.idToIndex[id] = idx
}

func main() {
	log.Println("Lith Ranker: Starting up...")

	// 1. Lấy cấu hình từ biến môi trường
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("FATAL: DATABASE_URL is required.")
	}

	intervalMinutes := DefaultInterval
	if val := os.Getenv("LITH_INTERVAL_MINUTES"); val != "" {
		if i, err := strconv.Atoi(val); err == nil && i > 0 {
			intervalMinutes = i
		}
	}

	maxIter := DefaultMaxIterations
	if val := os.Getenv("LITH_MAX_ITERATIONS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil && i > 0 {
			maxIter = i
		}
	}

	// 2. Kết nối Database
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("FATAL: Cannot connect to DB: %v", err)
	}
	defer pool.Close()
	log.Println("Lith Ranker: Connected to PostgreSQL.")

	// 3. Chờ một chút để Crawler chạy trước (nếu khởi động cùng lúc)
	log.Println("Lith Ranker: Warming up (30s)...")
	time.Sleep(30 * time.Second)

	// 4. Vòng lặp chính
	ticker := time.NewTicker(time.Duration(intervalMinutes) * time.Minute)
	defer ticker.Stop()

	// Chạy ngay lần đầu tiên
	runLithCycle(pool, maxIter)

	for range ticker.C {
		runLithCycle(pool, maxIter)
	}
}

func runLithCycle(pool *pgxpool.Pool, maxIter int) {
	log.Println("--- Starting Lith Rank Calculation Cycle ---")
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	if err := calculateLithRank(ctx, pool, maxIter); err != nil {
		log.Printf("ERROR: Lith calculation failed: %v", err)
	} else {
		log.Printf("SUCCESS: Lith Rank updated in %v", time.Since(start))
	}
	log.Println("--- Cycle Finished ---")
}

// calculateLithRank thực hiện thuật toán PageRank
func calculateLithRank(ctx context.Context, pool *pgxpool.Pool, maxIter int) error {
	// B1: Tải đồ thị từ DB vào RAM (Optimized)
	graph, err := loadGraph(ctx, pool)
	if err != nil {
		return err
	}

	count := len(graph.IDs)
	if count == 0 {
		log.Println("Graph is empty. Nothing to rank.")
		return nil
	}
	log.Printf("Graph loaded: %d pages.", count)

	// B2: Khởi tạo điểm số ban đầu (1.0 / N)
	initialScore := 1.0 / float64(count)
	for i := 0; i < count; i++ {
		graph.Scores[i] = initialScore
	}

	// B3: Lặp để tính toán (The core algorithm)
	// Công thức: PR(A) = (1-d)/N + d * Sum(PR(B)/L(B))
	baseScore := (1.0 - DampingFactor) / float64(count)

	for iter := 0; iter < maxIter; iter++ {
		for i := 0; i < count; i++ {
			incomingScore := 0.0
			// Iterate over indices of pages linking to i
			for _, sourceIdx := range graph.LinksIn[i] {
				// Access source page via index directly
				if graph.LinksOut[sourceIdx] > 0 {
					incomingScore += graph.Scores[sourceIdx] / float64(graph.LinksOut[sourceIdx])
				}
			}
			graph.NewScores[i] = baseScore + (DampingFactor * incomingScore)
		}

		// Swap scores
		copy(graph.Scores, graph.NewScores)
	}

	// Calculate Composite Score (Freshness + DepthIndex)
	now := time.Now().Unix()
	for i := 0; i < count; i++ {
		// Freshness: max(0, 1 - AgeInDays/365)
		ageSeconds := now - graph.CrawledAts[i]
		ageDays := float64(ageSeconds) / (24.0 * 3600.0)
		freshness := math.Max(0, 1.0-(ageDays/365.0))

		// DepthIndex: log10(TextLength)
		depthIndex := 0.0
		if graph.TextLengths[i] > 0 {
			depthIndex = math.Log10(float64(graph.TextLengths[i]))
		}

		// Store component scores
		graph.Freshness[i] = freshness
		graph.Quality[i] = depthIndex

		// Composite Formula
		graph.Scores[i] = (graph.Scores[i] * 0.8) + (freshness * 0.1) + (depthIndex * 0.1)
	}

	// B4: Lưu kết quả xuống DB (Optimized with Temp Table)
	return saveScores(ctx, pool, graph)
}

func loadGraph(ctx context.Context, pool *pgxpool.Pool) (*CompactGraph, error) {
	// Estimate capacity
	var count int
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM crawled_pages").Scan(&count)
	if err != nil {
		count = 10000 // Fallback
	}

	graph := NewCompactGraph(count)

	// Lấy danh sách các trang (Nodes)
	// Only fetch strictly necessary fields.
	rows, err := pool.Query(ctx, "SELECT id, COALESCE(text_length, 0), COALESCE(crawled_at, CURRENT_TIMESTAMP) FROM crawled_pages")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var textLength int
		var crawledAt time.Time

		if err := rows.Scan(&id, &textLength, &crawledAt); err == nil {
			graph.AddNode(id, textLength, crawledAt)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Lấy danh sách liên kết (Edges)
	// Optimization: We use IDs directly if possible.
	// But `page_links` only has URLs. 
	// To avoid full table joins which are slow, we rely on the JOIN query.
	// However, we can optimize by ensuring indices are used.
	
	// Note: The previous JOIN query on string URLs is indeed slow if not indexed, but we have indices.
	// The main bottleneck described by user was 'loading entire graph into RAM' (map pointers)
	// and 'Full Table Scan' (SELECT id FROM crawled_pages).
	// We've addressed the RAM issue with CompactGraph.
	// The 'Full Table Scan' on Nodes is unavoidable for PageRank.
	
	linkQuery := `
		SELECT s.id, t.id 
		FROM page_links pl
		JOIN crawled_pages s ON pl.source_url = s.url
		JOIN crawled_pages t ON pl.target_url = t.url
	`
	// Typically we would prefer page_links to use IDs, but that requires schema change.
	// We proceed with the existing query.
	
	linkRows, err := pool.Query(ctx, linkQuery)
	if err != nil {
		return nil, err
	}
	defer linkRows.Close()

	for linkRows.Next() {
		var srcID, tgtID int64
		if err := linkRows.Scan(&srcID, &tgtID); err == nil {
			srcIdx, srcOk := graph.idToIndex[srcID]
			tgtIdx, tgtOk := graph.idToIndex[tgtID]

			if srcOk && tgtOk {
				graph.LinksOut[srcIdx]++
				graph.LinksIn[tgtIdx] = append(graph.LinksIn[tgtIdx], srcIdx)
			}
		}
	}

	if err := linkRows.Err(); err != nil {
		return nil, err
	}

	// Clean up map to free memory before ranking
	graph.idToIndex = nil
	// Force GC if needed? Go usually handles it.
	
	return graph, nil
}

func saveScores(ctx context.Context, pool *pgxpool.Pool, graph *CompactGraph) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "CREATE TEMP TABLE lith_scores_temp (id BIGINT, score FLOAT, freshness FLOAT, quality FLOAT) ON COMMIT DROP")
	if err != nil {
		return fmt.Errorf("creating temp table: %w", err)
	}

	// Prepare data for COPY
	rows := [][]interface{}{}
	for i := 0; i < len(graph.IDs); i++ {
		rows = append(rows, []interface{}{graph.IDs[i], graph.Scores[i], graph.Freshness[i], graph.Quality[i]})
	}

	count, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"lith_scores_temp"},
		[]string{"id", "score", "freshness", "quality"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copying to temp table: %w", err)
	}
	log.Printf("Copied %d rows to temp table.", count)

	updateQuery := `
		UPDATE crawled_pages
		SET lith_score = t.score,
		    freshness_score = t.freshness,
		    quality_score = t.quality
		FROM lith_scores_temp t
		WHERE crawled_pages.id = t.id
	`
	cmdTag, err := tx.Exec(ctx, updateQuery)
	if err != nil {
		return fmt.Errorf("updating main table: %w", err)
	}

	log.Printf("Updated %d rows in crawled_pages.", cmdTag.RowsAffected())
	return tx.Commit(ctx)
}
