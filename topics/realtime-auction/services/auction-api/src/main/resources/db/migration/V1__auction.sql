-- Real-Time Auction schema. One Postgres database is the whole correctness
-- boundary: `SELECT ... FOR UPDATE` on the auctions row serialises every bid,
-- which is what gives bids their authoritative order (decision 001).
CREATE TABLE auctions (
    id            bigserial      PRIMARY KEY,
    title         text           NOT NULL,
    start_price   numeric(19,2)  NOT NULL CHECK (start_price >= 0),
    highest_bid   numeric(19,2),
    highest_bidder text,
    status        text           NOT NULL DEFAULT 'OPEN'
                    CHECK (status IN ('OPEN', 'CLOSED')),
    ends_at       timestamptz    NOT NULL,
    bid_count     int            NOT NULL DEFAULT 0,
    created_at    timestamptz    NOT NULL DEFAULT now()
);

-- Every bid attempt is recorded — accepted and rejected alike — so the audit
-- trail explains why a losing bid lost. seq is the per-auction bid order and is
-- only assigned to accepted bids: seq == bid_count == position in the stream.
CREATE TABLE bids (
    id         bigserial      PRIMARY KEY,
    auction_id bigint         NOT NULL REFERENCES auctions (id),
    bidder     text           NOT NULL,
    amount     numeric(19,2)  NOT NULL CHECK (amount > 0),
    accepted   boolean        NOT NULL,
    seq        int,
    at         timestamptz    NOT NULL DEFAULT now()
);

CREATE INDEX bids_auction ON bids (auction_id, accepted, seq);
