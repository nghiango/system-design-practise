# 002 — Redis Lua token bucket, and at-least-once + idempotency instead of "exactly-once"

## Context

Provider quotas (e.g. 10 msgs/s per channel) must hold across *all* worker
replicas, and the pipeline must never deliver a notification twice — while
Kafka only guarantees at-least-once delivery.

## Options — rate limiter

- **Redis Lua token bucket** — one hash `rate:<channel> = {tokens, ts}`; a
  script refills from elapsed Redis `TIME` and decrements atomically. O(1), one
  round trip, correct under any replica count, no client clock skew.
- **In-memory per-instance limiter** — zero infra, but N replicas get N× the
  quota and limits reset on restart. Rejected.
- **Postgres counter** — durable but puts write load on the send hot path and
  needs the same atomic-update problem solved. Rejected.

## Options — delivery guarantee

- **At-least-once + idempotency at every hand-off** — UNIQUE `idempotency_key`
  + `INSERT ... ON CONFLICT DO NOTHING` for submits; status-guarded UPDATEs
  (`WHERE status = ...`) for every transition; the `SENDING` claim is a
  compare-and-swap that a stale retry cannot pass.
- **Kafka exactly-once semantics (EOS)** — covers produce/consume through the
  broker but not the Postgres write or the provider call; the dedupe logic
  would still be required. Rejected as a *claim*, though transactions could
  complement it later.
- **At-most-once (commit offset first)** — a crash loses notifications.
  Rejected.

## Decision

- Lua bucket: capacity 10, refill 10/s, key `<prefix>rate:<channel>`, refilled
  lazily from Redis `TIME`, PEXPIRE 60s so idle channels leak nothing.
- Redis failure **fails throttled** (requeue, don't send) — sending without a
  token is how real providers ban senders; a stalled queue is visible and
  recoverable.
- Idempotent submit: conflict on `idempotency_key` returns the stored `{id,
  status}` with 200 — clients retry freely.
- Guarded transitions: `QUEUED→SENDING→SENT`, `SENDING→QUEUED` (retry),
  `QUEUED|SENDING→DEAD`. Terminal rows can never move backwards, so a late
  retry delivery against SENT/DEAD is a no-op.

## Consequences

- Throughput is *intentionally* capped at 10 msgs/s/channel — bursts queue in
  Kafka instead of erroring.
- A worker crash between SENDING and SENT leaves a row in `SENDING` forever —
  a real system adds a reaper for stale SENDING rows; documented gap for the
  exercise.
- Two duplicate *provider sends* remain theoretically possible if a worker
  dies after sending but before the SENT update and a redelivery re-sends —
  unavoidable without a provider-side idempotency key; noted honestly rather
  than claimed solved.
