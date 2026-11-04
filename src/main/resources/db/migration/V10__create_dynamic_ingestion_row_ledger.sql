CREATE TABLE dynamic_ingestion_rows (
    job_id UUID NOT NULL REFERENCES ingestion_jobs(id),
    row_number BIGINT NOT NULL,
    target_schema VARCHAR(63) NOT NULL,
    target_table VARCHAR(63) NOT NULL,
    outcome VARCHAR(20) NOT NULL CHECK (outcome IN ('INSERTED', 'REJECTED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, row_number)
);

CREATE INDEX idx_dynamic_ingestion_rows_job
    ON dynamic_ingestion_rows(job_id);
