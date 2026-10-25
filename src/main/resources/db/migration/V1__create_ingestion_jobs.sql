CREATE TABLE ingestion_jobs (
                                id UUID PRIMARY KEY,
                                file_name VARCHAR(255) NOT NULL,
                                file_path VARCHAR(1000) NOT NULL,
                                status VARCHAR(40) NOT NULL,
                                total_rows BIGINT NOT NULL DEFAULT 0,
                                processed_rows BIGINT NOT NULL DEFAULT 0,
                                failed_rows BIGINT NOT NULL DEFAULT 0,
                                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                started_at TIMESTAMPTZ,
                                completed_at TIMESTAMPTZ
);
