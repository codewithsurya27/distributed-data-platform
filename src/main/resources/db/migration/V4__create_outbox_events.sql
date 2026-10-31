CREATE TABLE outbox_events (
                               id UUID PRIMARY KEY,
                               aggregate_id UUID NOT NULL,
                               event_type VARCHAR(100) NOT NULL,
                               payload JSONB NOT NULL,
                               status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
                               created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                               published_at TIMESTAMPTZ,
                               attempts INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_outbox_pending
    ON outbox_events(status, created_at);