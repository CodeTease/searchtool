package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	// Bỏ "io" vì Go không thích import thừa

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// Struct để chứa kết quả tìm kiếm
type SearchResult struct {
	URL        string `json:"url"`
	Title      string `json:"title"`
	Snippet    string `json:"snippet"` // Đoạn trích nổi bật
	MetaDesc   string `json:"meta_description"`
}

// Struct ánh xạ với config.json (dùng con trỏ để merge cho dễ)
type CrawlerConfig struct {
	StartUrls               *[]string `json:"start_urls,omitempty"`
	MaxDepth                *int      `json:"max_depth,omitempty"`
	MaxConcurrentRequests   *int      `json:"max_concurrent_requests,omitempty"`
	DelayPerDomain          *float64  `json:"delay_per_domain,omitempty"`
	UserAgent               *string   `json:"user_agent,omitempty"`
	OutputFile              *string   `json:"output_file,omitempty"`
	MaxPages                *int      `json:"max_pages,omitempty"`
	MaxRequestsPerMinute    *int      `json:"max_requests_per_minute,omitempty"`
	MaxRetries              *int      `json:"max_retries,omitempty"`
	SaveToDb                *bool     `json:"save_to_db,omitempty"`
	SaveToJson              *bool     `json:"save_to_json,omitempty"`
	SslVerify               *bool     `json:"ssl_verify,omitempty"`
	DbSslCaCertPath         *string   `json:"db_ssl_ca_cert_path,omitempty"`
	DbBatchSize             *int      `json:"db_batch_size,omitempty"`
	MinioStorage            *struct {
		Enabled     *bool   `json:"enabled,omitempty"`
		Endpoint    *string `json:"endpoint,omitempty"`
		BucketName  *string `json:"bucket_name,omitempty"`
		Secure      *bool   `json:"secure,omitempty"`
	} `json:"minio_storage,omitempty"`
}


var dbPool *pgxpool.Pool
var configMutex sync.Mutex // Mutex để bảo vệ việc đọc/ghi file config

// Đường dẫn đến file config trong container (nhất quán)
const configPath = "/app/crawler/config.json"

// Cấu hình mặc định siêu nhẹ
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

// ensureConfigFileExists kiểm tra xem config.json có tồn tại không.
func ensureConfigFileExists() error {
	_, err := os.Stat(configPath)
	if os.IsNotExist(err) {
		log.Printf("Warning: %s not found. Creating default, lightweight config.", configPath)
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
	if err := ensureConfigFileExists(); err != nil {
		log.Fatalf("Fatal Error: Could not ensure config file exists or read permissions: %v\n", err)
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL must be set")
	}

	var err error
	dbPool, err = pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer dbPool.Close()

	log.Println("Successfully connected to PostgreSQL!")

	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	e.GET("/api/search", searchHandler)
	e.GET("/api/config", getConfigHandler)
	e.POST("/api/config", updateConfigHandler)

	// Sửa đường dẫn static (do đã mount vào /app)
	e.Static("/", "/app/go-backend/static")

	log.Println("Starting TeaserSearch Backend on :8080...")
	e.Logger.Fatal(e.Start(":8080"))
}

// searchHandler: Xử lý logic tìm kiếm Full-Text Search
func searchHandler(c echo.Context) error {
	query := c.QueryParam("q")
	if query == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "query parameter 'q' is required"})
	}

	sqlQuery := `
		SELECT 
			url, 
			title, 
			meta_description, 
			ts_headline('english', body_text, to_tsquery('english', $1), 'StartSel=<b>,StopSel=</b>') as snippet 
		FROM 
			crawled_pages 
		WHERE 
			tsv_document @@ to_tsquery('english', $1) 
		ORDER BY 
			ts_rank(tsv_document, to_tsquery('english', $1)) DESC 
		LIMIT 20;
	`

	rows, err := dbPool.Query(context.Background(), sqlQuery, query)
	if err != nil {
		log.Printf("Query failed: %v\n", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "search failed"})
	}
	defer rows.Close()

	results := []SearchResult{}
	for rows.Next() {
		var res SearchResult
		if err := rows.Scan(&res.URL, &res.Title, &res.MetaDesc, &res.Snippet); err != nil {
			log.Printf("Row scan failed: %v\n", err)
			continue
		}
		results = append(results, res)
	}

	return c.JSON(http.StatusOK, results)
}

// getConfigHandler: Đọc cấu hình hiện tại từ file config.json
func getConfigHandler(c echo.Context) error {
	configMutex.Lock()
	defer configMutex.Unlock()

	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Printf("Error reading config file: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Could not read crawler config file"})
	}

	var currentConfig map[string]interface{}
	if err := json.Unmarshal(data, &currentConfig); err != nil {
		log.Printf("Error unmarshalling config JSON: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Invalid config file format"})
	}

	// === SỬA LỖI: THÊM ANTI-CACHE HEADERS ===
	// Ra lệnh cho trình duyệt KHÔNG BAO GIỜ cache API này
	c.Response().Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
	c.Response().Header().Set("Pragma", "no-cache")
	c.Response().Header().Set("Expires", "0")
	// ======================================

	return c.JSON(http.StatusOK, currentConfig)
}

// updateConfigHandler: Cập nhật cấu hình vào file config.json
func updateConfigHandler(c echo.Context) error {
	configMutex.Lock()
	defer configMutex.Unlock()

	// 1. Đọc cấu hình hiện tại
	currentData, err := os.ReadFile(configPath)
	if err != nil {
		log.Printf("Error reading current config: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Could not read current config"})
	}

	var currentConfig map[string]interface{}
	if err := json.Unmarshal(currentData, &currentConfig); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Invalid current config format"})
	}

	// 2. Phân tích cú pháp cấu hình cập nhật từ Body
	var updates map[string]interface{}
	if err := c.Bind(&updates); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid JSON payload"})
	}

	// 3. Hợp nhất (Merge) cấu hình
	for key, value := range updates {
		// Xử lý đặc biệt cho minio_storage (nested object)
		if key == "minio_storage" {
			if minioUpdates, ok := value.(map[string]interface{}); ok {
				if currentMinio, ok := currentConfig["minio_storage"].(map[string]interface{}); ok {
					for minioKey, minioValue := range minioUpdates {
						currentMinio[minioKey] = minioValue
					}
					currentConfig["minio_storage"] = currentMinio
				} else {
					currentConfig["minio_storage"] = minioUpdates
				}
			}
		} else {
			currentConfig[key] = value
		}
	}


	// 4. Ghi lại cấu hình đã hợp nhất vào file
	updatedData, err := json.MarshalIndent(currentConfig, "", "  ")
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Error marshalling updated config"})
	}
	
	if err := os.WriteFile(configPath, updatedData, 0644); err != nil {
		log.Printf("Error writing updated config file: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Could not write updated config file"})
	}

	log.Println("Crawler config updated successfully.")
	return c.JSON(http.StatusOK, currentConfig)
}