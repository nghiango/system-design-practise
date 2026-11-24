CREATE TABLE notifications (
    id              bigserial    PRIMARY KEY,
    idempotency_key text         NOT NULL UNIQUE,
    channel         text         NOT NULL,
    target          text         NOT NULL,
    body            text         NOT NULL,
    status          text         NOT NULL DEFAULT 'QUEUED',
    attempts        int          NOT NULL DEFAULT 0,
    created_at      timestamptz  NOT NULL DEFAULT now(),
    updated_at      timestamptz  NOT NULL DEFAULT now()
);
