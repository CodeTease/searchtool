package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/meilisearch/meilisearch-go"
)

// SearchResult struct
type SearchResult struct {
	URL       string  `json:"url"`
	Title     string  `json:"title"`
	Snippet   string  `json:"snippet"`
	MetaDesc  string  `json:"meta_description"`
	LithScore float64 `json:"lith_score"` // Hiển thị điểm số Lith
}

var dbPool *pgxpool.Pool
var meiliClient meilisearch.ServiceManager

const configPath = "/app/crawler/config.json"

// Struct ánh xạ với config.json
type CrawlerConfig struct {
	StartUrls             *[]string `json:"start_urls,omitempty"`
	MaxDepth              *int      `json:"max_depth,omitempty"`
	MaxConcurrentRequests *int      `json:"max_concurrent_requests,omitempty"`
	DelayPerDomain        *float64  `json:"delay_per_domain,omitempty"`
	UserAgent             *string   `json:"user_agent,omitempty"`
	OutputFile            *string   `json:"output_file,omitempty"`
	MaxPages              *int      `json:"max_pages,omitempty"`
	MaxRequestsPerMinute  *int      `json:"max_requests_per_minute,omitempty"`
	MaxRetries            *int      `json:"max_retries,omitempty"`
	SaveToDb              *bool     `json:"save_to_db,omitempty"`
	SaveToJson            *bool     `json:"save_to_json,omitempty"`
	SslVerify             *bool     `json:"ssl_verify,omitempty"`
	DbSslCaCertPath       *string   `json:"db_ssl_ca_cert_path,omitempty"`
	DbBatchSize           *int      `json:"db_batch_size,omitempty"`
	MinioStorage          *struct {
		Enabled    *bool   `json:"enabled,omitempty"`
		Endpoint   *string `json:"endpoint,omitempty"`
		BucketName *string `json:"bucket_name,omitempty"`
		Secure     *bool   `json:"secure,omitempty"`
	} `json:"minio_storage,omitempty"`
}

// Cấu hình mặc định (Fallback)
const defaultConfigFileContent = `{
  "start_urls": ["https://teaserverse.dev"],
  "max_depth": 1,
  "max_concurrent_requests": 5,
  "delay_per_domain": 1.0,
  "user_agent": "TeaserBot/LocalDev",
  "max_pages": 5,
  "save_to_db": true,
  "save_to_json": false,
  "ssl_verify": false,
  "minio_storage": {"enabled": false}
}`

func ensureConfigFileExists() error {
	_, err := os.Stat(configPath)
	if os.IsNotExist(err) {
		log.Printf("Warning: %s not found. Creating default config.", configPath)
		if err := os.WriteFile(configPath, []byte(defaultConfigFileContent), 0644); err != nil {
			return err
		}
		log.Println("Default config file created successfully.")
	} else if err != nil {
		return err
	}
	return nil
}

func main() {
	// ... (Code kết nối DB) ...
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL must be set")
	}
	var err error
	dbPool, err = pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("DB Error: %v", err)
	}
	defer dbPool.Close()

	// Ensure config file exists on startup
	if err := ensureConfigFileExists(); err != nil {
		log.Printf("Failed to ensure config file: %v", err)
	}

	// --- Meilisearch Setup ---
	meiliHost := os.Getenv("MEILI_HOST")
	if meiliHost == "" {
		meiliHost = "http://meilisearch:7700"
	}
	meiliKey := os.Getenv("MEILI_API_KEY")

	meiliClient = meilisearch.New(meiliHost, meilisearch.WithAPIKey(meiliKey))

	// --- Enforce Ranking Rules on Startup ---
	go func() {
		// Chờ Meili khởi động
		for i := 0; i < 5; i++ {
			if _, err := meiliClient.Health(); err == nil {
				log.Println("Configuring Meilisearch Ranking Rules for Lith Ranker...")
				meiliClient.Index("pages").UpdateSettings(&meilisearch.Settings{
					RankingRules: []string{
						"words",
						"typo",
						"proximity",
						"attribute",
						"sort",
						"exactness",
						"lith_score:desc",
					},
					SortableAttributes: []string{"lith_score", "crawled_at"},
				})
				return
			}
			// Sleep handled by Docker restart policy or user patience
		}
	}()

	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Configurable CORS
	allowedOrigins := []string{"*"}
	if envOrigins := os.Getenv("CORS_ALLOWED_ORIGINS"); envOrigins != "" {
		// Split by comma if needed, or just take the string if it's single
		// Echo CORS middleware takes a list. For now we assume a single or handle manually,
		// but typically we'd parse. Let's keep it simple: if set, use it.
		// If user passes "http://a.com,http://b.com", we might need to split.
		// But for now let's just use what's passed if it's not empty, assuming user knows format?
		// Echo expects []string.
		// Let's rely on standard practice: default to * for dev, stricter for prod.
		allowedOrigins = []string{envOrigins}
	}

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: allowedOrigins,
		AllowMethods: []string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete},
	}))

	e.GET("/api/search", searchHandler)

	// Admin API with Basic Auth
	adminGroup := e.Group("/api/config")
	adminGroup.Use(middleware.BasicAuth(func(username, password string, c echo.Context) (bool, error) {
		adminUser := os.Getenv("ADMIN_USERNAME")
		if adminUser == "" {
			adminUser = "admin" // Default fallback, but ENV is preferred
		}
		adminPass := os.Getenv("ADMIN_PASSWORD")

		// Require ADMIN_PASSWORD to be set for security
		if adminPass == "" {
			return false, nil
		}

		if subtle.ConstantTimeCompare([]byte(username), []byte(adminUser)) == 1 &&
			subtle.ConstantTimeCompare([]byte(password), []byte(adminPass)) == 1 {
			return true, nil
		}
		return false, nil
	}))

	adminGroup.GET("", getConfigHandler)
	adminGroup.POST("", updateConfigHandler)

	e.Static("/", "/app/go-backend/static")

	e.Logger.Fatal(e.Start(":8080"))
}

func searchHandler(c echo.Context) error {
	query := c.QueryParam("q")
	if query == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "query param 'q' required"})
	}

	searchRequest := &meilisearch.SearchRequest{
		Limit:                 20,
		AttributesToRetrieve:  []string{"url", "title", "meta_description", "lith_score"},
		AttributesToHighlight: []string{"*"},
		HighlightPreTag:       "<b>",
		HighlightPostTag:      "</b>",
		AttributesToCrop:      []string{"body_text"},
		CropLength:            150,
	}

	searchRes, err := meiliClient.Index("pages").Search(query, searchRequest)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "search failed"})
	}

	results := []SearchResult{}
	for _, hit := range searchRes.Hits {
		var hitMap map[string]interface{}
		hitJSON, _ := json.Marshal(hit)
		json.Unmarshal(hitJSON, &hitMap)

		res := SearchResult{}
		if url, ok := hitMap["url"].(string); ok {
			res.URL = url
		}
		if title, ok := hitMap["title"].(string); ok {
			res.Title = title
		}
		if meta, ok := hitMap["meta_description"].(string); ok {
			res.MetaDesc = meta
		}
		if score, ok := hitMap["lith_score"].(float64); ok {
			res.LithScore = score
		}

		if formatted, ok := hitMap["_formatted"].(map[string]interface{}); ok {
			if hSnippet, ok := formatted["body_text"].(string); ok {
				res.Snippet = hSnippet
			}
		}
		if res.Snippet == "" {
			res.Snippet = res.MetaDesc
		}

		results = append(results, res)
	}

	return c.JSON(http.StatusOK, results)
}

func getConfigHandler(c echo.Context) error {
	file, err := os.Open(configPath)
	if err != nil {
		// If file doesn't exist, try ensuring it (or return default content)
		if os.IsNotExist(err) {
			return c.JSON(http.StatusOK, json.RawMessage(defaultConfigFileContent))
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to read config"})
	}
	defer file.Close()

	var config map[string]interface{}
	if err := json.NewDecoder(file).Decode(&config); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to parse config"})
	}

	return c.JSON(http.StatusOK, config)
}

func updateConfigHandler(c echo.Context) error {
	var newConfig map[string]interface{}
	if err := c.Bind(&newConfig); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
	}

	// Simple atomic write simulation: write to temp file then rename.
	// This helps with some race conditions but shared volume issues persist if multiple replicas write.
	// For now, this improves local atomicity.

	data, err := json.MarshalIndent(newConfig, "", "  ")
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to marshal config"})
	}

	tmpPath := configPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to write temp config"})
	}

	if err := os.Rename(tmpPath, configPath); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to swap config file"})
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "updated"})
}
