
CREATE TABLE customer_records (
                                  id UUID PRIMARY KEY,
                                  job_id UUID NOT NULL REFERENCES ingestion_jobs(id),
                                  customer_id VARCHAR(100) NOT NULL,
                                  name VARCHAR(255) NOT NULL,
                                  email VARCHAR(320) NOT NULL,
                                  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                  CONSTRAINT uk_customer_records_job_customer
                                      UNIQUE (job_id, customer_id)
);

CREATE INDEX idx_customer_records_job_id
    ON customer_records(job_id);

CREATE TABLE ingestion_errors (
                                  id UUID PRIMARY KEY,
                                  job_id UUID NOT NULL REFERENCES ingestion_jobs(id),
                                  row_number BIGINT NOT NULL,
                                  error_code VARCHAR(100) NOT NULL,
                                  error_message VARCHAR(1000) NOT NULL,
                                  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ingestion_errors_job_row
    ON ingestion_errors(job_id, row_number);
