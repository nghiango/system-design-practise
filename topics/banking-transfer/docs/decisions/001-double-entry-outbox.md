# 001 — One Postgres transaction, double-entry ledger, transactional outbox

## Context

A transfer must (a) debit one account and credit another atomically, (b) leave an
audit trail that can prove no money was created or destroyed, and (c) publish a
Kafka event that is guaranteed to exist iff the transfer committed. Under
concurrency the same two accounts can be hit from both directions at once.

## Options

- **Single Postgres transaction: accounts + ledger + outbox** — one
  `@Transactional` unit does the idempotency insert, locks both account rows
  `FOR UPDATE` in ascending id order, checks funds, updates balances, writes the
  DEBIT/CREDIT ledger pair and the outbox row. Commit is atomic across all six
  writes. Chosen.
- **2PC / saga across services** — split accounts and ledger (or "debit service"
  + "credit service") and coordinate. Buys nothing: the data already lives in
  one schema; pays for it with coordinator failure modes, compensating
  transactions and partial-commit recovery. Rejected — the correct
  microservice boundary here is the database itself.
- **Best-effort `kafka.send` after commit** — loses the event on any crash
  between commit and send, and (worse) silently: nothing records that an event
  was owed. Rejected; the outbox turns "the event exists" into durable data.
- **Event sourcing** — the ledger IS the state; balances are folds. Real
  double-entry systems are built this way, but every funds check then needs a
  consistent snapshot of the fold, and reads pay the fold cost. The mutable
  `accounts` row + immutable ledger pair gives the same verifiability with
  trivial reads. Rejected for this exercise; noted as the honest next step.
- **SELECT-check-then-UPDATE** — under READ COMMITTED two concurrent debits both
  see the old balance, both pass the check, and one update is lost. Rejected:
  correctness requires the `FOR UPDATE` lock anyway, so the check must happen
  *under* the lock.
- **Lock rows in request order (from, then to)** — transfer A→B locks A then B;
  transfer B→A locks B then A; each then waits on the other's row → deadlock.
  Postgres detects and kills one, but turning a normal code path into a deadlock
  detector round-trip is wrong. Rejected: lock `min(id)` then `max(id)` so both
  directions contend on the same first lock — one waits, nobody dies.

## Decision

All money movement happens inside ONE Postgres transaction that writes
`transfers` + `accounts` updates + a two-row `ledger` entry + `outbox`, with
both account rows locked `FOR UPDATE` **in ascending account-id order**. A
scheduled relay publishes unpublished outbox rows
(`FOR UPDATE SKIP LOCKED`, `LIMIT 100`, key = transferId) and marks them
published — at-least-once, with consumer-side dedupe (decision 002).

## Consequences

- A committed transfer always has its event; a lost event is impossible by
  construction — only a delayed one.
- Ledger `balance_after` per entry makes `Σ signed entries == balance` a
  checkable invariant → the reconciliation endpoint.
- Hot-account throughput is bounded by row-lock serialisation; that is the
  intended semantics (correctness over parallelism on one account).
- Relay redelivery is expected; consumers must be idempotent.
