
CREATE TABLE processed_dlt_events (
                                      event_id UUID PRIMARY KEY,
                                      job_id UUID NOT NULL REFERENCES ingestion_jobs(id),
                                      handled_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_processed_dlt_events_job_id
    ON processed_dlt_events(job_id);
