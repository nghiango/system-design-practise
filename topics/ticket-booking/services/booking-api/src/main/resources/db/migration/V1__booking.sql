CREATE TABLE events (
    id         bigserial     PRIMARY KEY,
    name       text          NOT NULL,
    price      numeric       NOT NULL,
    created_at timestamptz   NOT NULL DEFAULT now()
);

-- One row per sellable seat. The (event_id, seat) primary key IS the inventory:
-- a concurrent hold can only win by changing status in a single atomic UPDATE.
CREATE TABLE seats (
    event_id        bigint       NOT NULL REFERENCES events (id),
    seat            text         NOT NULL,
    status          text         NOT NULL DEFAULT 'FREE'
                    CHECK (status IN ('FREE', 'HELD', 'BOOKED')),
    hold_owner      text,
    hold_expires_at timestamptz,
    booked_by       text,
    PRIMARY KEY (event_id, seat)
);

CREATE INDEX seats_expired_holds ON seats (hold_expires_at) WHERE status = 'HELD';

-- Simulated payment ledger; one row per confirm attempt.
CREATE TABLE payments (
    id         bigserial     PRIMARY KEY,
    event_id   bigint        NOT NULL REFERENCES events (id),
    seat       text          NOT NULL,
    owner      text          NOT NULL,
    amount     numeric       NOT NULL,
    status     text          NOT NULL,
    created_at timestamptz   NOT NULL DEFAULT now()
);

-- Append-only audit trail written by booking-worker from booking.events.
CREATE TABLE booking_history (
    id         bigserial     PRIMARY KEY,
    event_type text          NOT NULL,
    event_id   bigint        NOT NULL,
    seat       text,
    owner      text,
    at         timestamptz   NOT NULL DEFAULT now()
);

CREATE INDEX booking_history_event ON booking_history (event_id, id);
