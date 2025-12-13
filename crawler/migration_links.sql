-- Migrate to Integer IDs if not already done.
-- This script should be idempotent-ish or handled by application logic to not run if table is already correct.

DO $$
BEGIN
    -- Check if column source_id exists in page_links
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'page_links' AND column_name = 'source_id'
    ) THEN
        -- Drop old table (assumes data loss is acceptable for this migration or table is empty/expendable in dev)
        DROP TABLE IF EXISTS page_links;

        -- Create new table using Integer IDs
        CREATE TABLE page_links (
            source_id BIGINT NOT NULL,
            target_id BIGINT NOT NULL,
            PRIMARY KEY (source_id, target_id),
            FOREIGN KEY (source_id) REFERENCES crawled_pages(id) ON DELETE CASCADE,
            FOREIGN KEY (target_id) REFERENCES crawled_pages(id) ON DELETE CASCADE
        );

        -- Indices for performance
        CREATE INDEX IF NOT EXISTS source_id_idx ON page_links (source_id);
        CREATE INDEX IF NOT EXISTS target_id_idx ON page_links (target_id);
    END IF;
END
$$;
