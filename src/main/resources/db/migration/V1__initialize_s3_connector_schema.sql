-- Baseline schema for s3-connector crawler state and target configuration.
CREATE TABLE IF NOT EXISTS s3_datasource_configs (
    datasource_id VARCHAR(255) PRIMARY KEY,
    api_key TEXT NOT NULL,
    s3_config JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    version BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_s3_datasource_configs_created_at
    ON s3_datasource_configs (created_at);

-- Crawling target configuration
CREATE TABLE IF NOT EXISTS s3_crawl_targets (
    id BIGSERIAL PRIMARY KEY,
    datasource_id VARCHAR(255) NOT NULL,
    target_name VARCHAR(255) NOT NULL,
    bucket VARCHAR(512) NOT NULL,
    object_prefix VARCHAR(2048),
    crawl_mode VARCHAR(20) NOT NULL,
    failure_allowance INTEGER NOT NULL,
    max_keys_per_request INTEGER NOT NULL,
    enabled BOOLEAN NOT NULL,
    last_crawl_at TIMESTAMP WITH TIME ZONE,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT uk_s3_crawl_targets_bucket_prefix
        UNIQUE (datasource_id, bucket, object_prefix)
);

CREATE INDEX IF NOT EXISTS idx_s3_crawl_targets_datasource
    ON s3_crawl_targets (datasource_id);

CREATE INDEX IF NOT EXISTS idx_s3_crawl_targets_bucket
    ON s3_crawl_targets (bucket);

CREATE INDEX IF NOT EXISTS idx_s3_crawl_targets_enabled
    ON s3_crawl_targets (enabled);

CREATE INDEX IF NOT EXISTS idx_s3_crawl_targets_mode
    ON s3_crawl_targets (crawl_mode);

-- Object crawl state tracking
CREATE TABLE IF NOT EXISTS s3_crawl_state (
    id BIGSERIAL PRIMARY KEY,
    datasource_id VARCHAR(255) NOT NULL,
    bucket VARCHAR(512) NOT NULL,
    object_key VARCHAR(4096) NOT NULL,
    object_version_id VARCHAR(255) NOT NULL,
    object_etag VARCHAR(255),
    size_bytes BIGINT NOT NULL,
    last_modified TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) NOT NULL,
    attempt_count INTEGER NOT NULL,
    failure_allowance INTEGER NOT NULL,
    crawl_source VARCHAR(20) NOT NULL,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    fingerprint VARCHAR(255),
    last_error TEXT,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT uk_s3_crawl_state_object
        UNIQUE (datasource_id, bucket, object_key, object_version_id)
);

CREATE INDEX IF NOT EXISTS idx_s3_crawl_state_datasource_bucket
    ON s3_crawl_state (datasource_id, bucket);

CREATE INDEX IF NOT EXISTS idx_s3_crawl_state_status
    ON s3_crawl_state (status);

CREATE INDEX IF NOT EXISTS idx_s3_crawl_state_retry_at
    ON s3_crawl_state (next_retry_at);

CREATE INDEX IF NOT EXISTS idx_s3_crawl_state_updated_at
    ON s3_crawl_state (updated_at);

-- Ensure unique constraints are present on pre-existing tables (e.g., update-based deployments).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class
        WHERE relname = 's3_crawl_targets'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'uk_s3_crawl_targets_bucket_prefix'
                AND conrelid = 's3_crawl_targets'::regclass
        ) THEN
            ALTER TABLE s3_crawl_targets
                ADD CONSTRAINT uk_s3_crawl_targets_bucket_prefix
                UNIQUE (datasource_id, bucket, object_prefix);
        END IF;
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_class
        WHERE relname = 's3_crawl_state'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'uk_s3_crawl_state_object'
                AND conrelid = 's3_crawl_state'::regclass
        ) THEN
            ALTER TABLE s3_crawl_state
                ADD CONSTRAINT uk_s3_crawl_state_object
                UNIQUE (datasource_id, bucket, object_key, object_version_id);
        END IF;
    END IF;
END
$$;
