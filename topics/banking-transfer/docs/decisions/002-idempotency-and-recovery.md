# 002 — Idempotency keys and at-least-once recovery with consumer dedupe

## Context

Clients retry. Networks time out after the server committed. Workers crash
between "sent" and "marked sent". Every one of those produces a *duplicate* —
and in a money system a duplicate write is a lost-matter incident. The design
must make duplication harmless at each boundary.

## Options

- **Client-supplied `idempotency_key` + `INSERT ... ON CONFLICT DO NOTHING`** —
  the unique index arbitrates before any lock is taken: first writer wins,
  everyone else reads the stored terminal row and returns it verbatim
  (`replayed:true`). No second balance update, no second ledger pair. Works for
  retries seconds later AND for N concurrent submissions of the same key —
  Postgres serialises the conflicting inserts on the index entry. Chosen.
- **Server-generated request id returned before commit** — the client can't
  safely retry a request whose outcome is unknown (timeout after commit = new
  id = double-post). Rejected.
- **Exactly-once Kafka semantics (transactions / EOS)** — heavyweight, and it
  only covers the broker hop; the outbox→relay→consumer chain still has a DB
  write at each end. Rejected: at-least-once + idempotent consumer covers the
  real failure set.
- **At-most-once relay (mark published, then send)** — a crash between mark and
  send loses the event permanently. Rejected; the relay marks published only
  *after* the send completes, inside the same transaction that holds the row
  locks, so a crash anywhere re-delivers.
- **Consumer dedupe by offset/partition bookkeeping** — fragile across
  rebalances and replays. Rejected in favour of a semantic key:
  `UNIQUE (transfer_id, account_id, entry)` on `ledger_audit` — the business
  identity of a ledger line IS the dedupe key, so `INSERT ... ON CONFLICT DO
  NOTHING` makes redelivery a no-op with no extra state machine.

## Decision

- `transfers.idempotency_key UNIQUE`; the transfer transaction opens with
  `INSERT ... ON CONFLICT DO NOTHING` — conflict = replay the stored row.
- Relay = at-least-once by construction (`SKIP LOCKED` claim → send → mark
  published, crash-safe).
- Consumer = idempotent by construction (`ON CONFLICT DO NOTHING` on
  `(transfer_id, account_id, entry)`).
- Rejections are durable too: a `REJECTED` row + `transfer.rejected` outbox
  event commit like any other outcome, so a retried rejection replays REJECTED
  instead of re-evaluating against a changed balance.

## Consequences

- Replay response is byte-identical (same id, same terminal status) — clients
  can treat 200+replayed as success-equivalent to the original.
- `replayed:true` and the `transfers.replayed` counter make duplicate traffic
  observable.
- Duplicate Kafka delivery is expected and harmless; `ledger_audit` never
  double-counts, which is what keeps `projectionDrift` at zero.
- One edge accepted knowingly: two *different* keys for the "same" logical
  transfer are two transfers — key uniqueness is the client's contract.
