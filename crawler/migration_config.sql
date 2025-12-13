CREATE TABLE IF NOT EXISTS crawler_config (
    id SERIAL PRIMARY KEY,
    key VARCHAR(255) UNIQUE NOT NULL,
    value JSONB NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insert default config if not exists
INSERT INTO crawler_config (key, value) VALUES
('default', '{
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
}') ON CONFLICT (key) DO NOTHING;
