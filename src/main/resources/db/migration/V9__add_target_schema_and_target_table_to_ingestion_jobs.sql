ALTER TABLE ingestion_jobs
    ADD COLUMN target_schema VARCHAR(63),
    ADD COLUMN target_table VARCHAR(63);