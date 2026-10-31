-- Rename aggregate_id to job_id.
ALTER TABLE outbox_events
    RENAME COLUMN aggregate_id TO job_id;

-- Remove payload because the current OutboxEvent entity does not map it.
ALTER TABLE outbox_events
DROP COLUMN payload;

-- Add last_error to store publishing failure details.
ALTER TABLE outbox_events
    ADD COLUMN last_error VARCHAR(2000);