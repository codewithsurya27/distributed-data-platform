CREATE TABLE IF NOT EXISTS public.phase9_defaults (
                                                      id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                      name VARCHAR(100) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'NEW',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    );