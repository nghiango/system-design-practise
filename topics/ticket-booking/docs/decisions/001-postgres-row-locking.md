# 001 — Seat locking via a single guarded Postgres UPDATE

## Context

A seat must never be booked twice. At on-sale time hundreds of buyers can target
the *same* seat within the same second, so the claim must be atomic across all
booking-api replicas. We also need the claim to carry a TTL (`hold_expires_at`)
and an owner, in one shot.

## Options

- **Guarded UPDATE on the seat row** — `UPDATE seats SET status='HELD',
  hold_owner=?, hold_expires_at=now()+5min WHERE event_id=? AND seat=? AND
  (status='FREE' OR (status='HELD' AND hold_expires_at<now()))`. The row lock
  taken by the UPDATE serialises contenders; the WHERE clause is the claim
  condition. 1 row = won, 0 rows = lost. Chosen.
- **SELECT status, then UPDATE** — check-then-act. Under READ COMMITTED two
  transactions both read `FREE` and both proceed; fixing it needs
  `SELECT ... FOR UPDATE` (an explicit second lock round trip that does the
  same job as the guarded UPDATE, only slower) or SERIALIZABLE (retry storms on
  the hot seat). Rejected: strictly worse than doing the check inside the
  UPDATE.
- **Redis distributed lock (SET NX PX + Lua release)** — fast, but introduces a
  second system whose failure mode is *unsafe*: lock expiry races a slow
  client, failover can drop the lock, and the seat row still has to be written
  eventually — you end up reimplementing the guarded UPDATE anyway to be safe.
  Rejected for the exercise; the right shape at a scale where Postgres row
  contention is measured to be the bottleneck.
- **UNIQUE constraint on a `holds` table, then SELECT** — insert-first also
  serialises on the index, but every attempt writes a row (contention becomes
  index-page + WAL churn), losers generate dead tuples to vacuum, and you still
  need a second write to flip `seats.status`. Rejected.

## Decision

All seat transitions (`hold`, `confirm`, `release`, decline-release, sweeper
reclaim) are **one UPDATE each, guarded by the status (and owner/expiry) they
leave behind**. No SELECT ever decides whether a transition is allowed.

## Consequences

- Exactly-once claiming by construction; the integration test's 100-thread race
  is the evidence.
- The contended seat serialises — different seats never contend (different
  rows), so throughput scales with seat count, not request count.
- Postgres is the single authority for lock state; a crash releases row locks
  with the transaction, so there is no orphaned-lock state to reap (unlike a
  distributed lock service).
- Cost: every failed attempt still takes the row lock briefly — at on-sale
  bursts on *one* seat this is precisely the desired semantics; across a whole
  venue it is negligible.
