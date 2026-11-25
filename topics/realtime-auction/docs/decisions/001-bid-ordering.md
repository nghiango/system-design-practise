# 001 — Bid ordering: `SELECT ... FOR UPDATE` on the auction row

## Decision

`placeBid` is a single transaction: lock the auction row with `SELECT ... FOR UPDATE`, apply
every rule (closed? expired? beats highest?) against the just-locked state, then write the bid and
update `highest_*`/`bid_count` before committing. `seq = bid_count + 1` stamps the
lock-acquisition order durably. The Kafka update is published keyed by `auctionId`, which keeps
one auction's events on one partition — ordered, so watchers see bids in seq order.

## Why

An auction needs a *single* authoritative order for bids on one item. The auction row is already
the one place every bidder must go — making it the sequencer adds no new component, no new
failure mode, and the max-check is trivially exact because no one else can move the state while
the lock is held.

## Rejected alternatives

- **Kafka-ordering-only** — publish `{bid}` to `realtime-auction.bids` and let partition order
  decide the winner downstream. The HTTP request can't answer 200/409/410 synchronously: it would
  have to return "pending", or block on a projection round-trip, or acknowledge a bid that later
  loses. Ordering would be real but the *decision* would move off the request path and the API
  contract this exercise requires (200 {seq} / 409 / 410) could not be kept.
- **Distributed lock (Redis `SET NX` / Redlock)** — lease expiry, GC pauses while holding the
  lock, and fencing-token bookkeeping are failure modes the row lock does not have; and after
  "winning" the Redis lock you *still* need the DB read+write inside a critical section, so it
  adds a component without removing the serialisation.
- **Optimistic check-then-update / version column** — two contenders read the same highest under
  READ COMMITTED; CAS retries re-serialise on the hot row anyway, with extra abort/retry code and
  a less obvious order than "lock order is the order".
- **DB `SELECT MAX(seq)` under SERIALIZABLE** — heavier isolation for no benefit; FOR UPDATE on
  one row is the precise tool.

## Consequences

- One hot auction serialises at ~row-lock speed (the real ceiling — documented in capacity).
- `seq` is contiguous `1..N` per auction; rejected bids get no seq but are still recorded
  (`accepted = false`) — losing is part of the record.
- Publish-after-commit-update inside the txn means a Kafka outage can lose a *tick*, never a bid;
  `GET /api/auctions/{id}` remains the truth.
