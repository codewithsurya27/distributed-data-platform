CREATE TABLE processed_events (
                                  event_id UUID PRIMARY KEY,
                                  job_id UUID NOT NULL REFERENCES ingestion_jobs(id),
                                  status VARCHAR(30) NOT NULL,
                                  received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                  completed_at TIMESTAMPTZ,

                                  CONSTRAINT chk_processed_events_status
                                      CHECK (status IN ('IN_PROGRESS', 'COMPLETED'))
);

CREATE INDEX idx_processed_events_job_id
    ON processed_events(job_id);