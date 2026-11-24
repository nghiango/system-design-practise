-- Banking Money Transfer schema. The whole transfer boundary lives in ONE
-- Postgres database: accounts + double-entry ledger + outbox commit atomically
-- in a single transaction, so there is no 2PC anywhere (decision 001).
CREATE TABLE accounts (
    id         bigserial      PRIMARY KEY,
    owner      text           NOT NULL UNIQUE,
    -- CHECK is the last line of defence: no UPDATE can ever leave a negative
    -- balance, even if the service-layer funds check has a bug.
    balance    numeric(19,2)  NOT NULL DEFAULT 0 CHECK (balance >= 0),
    created_at timestamptz    NOT NULL DEFAULT now()
);

-- One row per transfer request, keyed by the client's idempotency key.
-- INSERT ... ON CONFLICT DO NOTHING makes the first writer the only writer:
-- a retried request replays the stored result instead of double-posting.
CREATE TABLE transfers (
    id              bigserial      PRIMARY KEY,
    idempotency_key text           NOT NULL UNIQUE,
    from_account    bigint         NOT NULL,
    to_account      bigint         NOT NULL,
    amount          numeric(19,2)  NOT NULL CHECK (amount > 0),
    status          text           NOT NULL DEFAULT 'INITIATED'
                    CHECK (status IN ('INITIATED', 'COMPLETED', 'REJECTED')),
    created_at      timestamptz    NOT NULL DEFAULT now()
);

-- Double-entry ledger: every COMPLETED transfer writes exactly two rows —
-- DEBIT on the source, CREDIT on the destination — each carrying the account
-- balance right after the entry. Sum of signed entries must equal balance;
-- that invariant is what /api/reconciliation verifies.
CREATE TABLE ledger (
    id            bigserial      PRIMARY KEY,
    -- Nullable: an account's opening CREDIT entry (booked at creation so that
    -- Σ signed entries == balance holds for reconciliation) has no transfer.
    transfer_id   bigint         REFERENCES transfers (id),
    account_id    bigint         NOT NULL REFERENCES accounts (id),
    entry         text           NOT NULL CHECK (entry IN ('DEBIT', 'CREDIT')),
    amount        numeric(19,2)  NOT NULL,
    balance_after numeric(19,2)  NOT NULL,
    created_at    timestamptz    NOT NULL DEFAULT now()
);

CREATE INDEX ledger_account ON ledger (account_id, id);

-- Transactional outbox: written in the SAME transaction as the ledger rows, so
-- a committed transfer can never lose its event. ledger-worker relays it to
-- Kafka at-least-once; consumers dedupe (decision 001).
CREATE TABLE outbox (
    id         bigserial    PRIMARY KEY,
    type       text         NOT NULL,
    payload    text         NOT NULL,
    published  boolean      NOT NULL DEFAULT false,
    created_at timestamptz  NOT NULL DEFAULT now()
);

-- Partial index keeps the relay scan O(unpublished), not O(outbox).
CREATE INDEX outbox_unpublished ON outbox (id) WHERE NOT published;

-- ledger-worker projection of published ledger entries. The
-- (transfer_id, account_id, entry) unique key IS the dedupe: at-least-once
-- delivery can re-offer the same event, the re-insert is a no-op.
CREATE TABLE ledger_audit (
    id            bigserial      PRIMARY KEY,
    transfer_id   bigint         NOT NULL,
    account_id    bigint         NOT NULL,
    entry         text           NOT NULL,
    amount        numeric(19,2)  NOT NULL,
    balance_after numeric(19,2)  NOT NULL,
    at            timestamptz    NOT NULL DEFAULT now(),
    UNIQUE (transfer_id, account_id, entry)
);

CREATE INDEX ledger_audit_account ON ledger_audit (account_id, id);
