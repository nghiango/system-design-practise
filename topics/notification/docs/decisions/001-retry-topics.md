# 001 — Retries via Kafka topics, not in-consumer sleep

## Context

Send attempts fail transiently (provider 5xx, throttling). A retry mechanism must:
not block other messages, survive worker restarts, and stay bounded.

## Options

- **Republish to a `notification.retry` topic** — the consumer acks the original
  event and produces `{id, attempt+1}` to a sibling topic consumed by the same
  listener. Redelivery is broker-driven; the consumer thread never sleeps.
- **`Thread.sleep` + reprocess in the consumer** — simplest code, but a sleeping
  thread stalls the whole partition (Kafka consumer processes a partition
  serially), every queued message behind a throttled one waits, and a crash
  mid-sleep replays from the last committed offset anyway. Rejected.
- **Delayed buckets** (retry-5s / retry-30s / retry-5m topics or a delay
  scheduler) — real back-off, but extra topics and delay infrastructure for a
  benefit the exercise doesn't need: the Redis token bucket already paces the
  retry loop to the send rate. Rejected for now; the right next step if a real
  provider demands exponential back-off.
- **DB `next_attempt_at` column + scheduler poll** — durable back-off, but adds a
  polling job and DB reads on the hot path. Rejected.

## Decision

- Transient failure & attempts < 3 → `UPDATE ... SET status='QUEUED',
  attempts=attempts+1` and republish to `notification.retry` (6 partitions, key
  = id).
- Attempts exhausted or permanent error → `status='DEAD'` + event on
  `notification.dlq`.
- **Rate-limit requeues do not consume an attempt**: a starved token bucket
  republishes the *unchanged* `{id, attempt}` event — throttling is flow
  control, not a provider failure. Only a consumed provider call burns budget.
- Requeues are immediate (no delay). The token bucket bounds the spin to the
  send rate, so a throttled message loops at most ~100ms per cycle at 10/s.

## Consequences

- `notification.retry` doubles as the throttle-overflow queue: under a burst it
  holds the messages the bucket won't let through yet.
- A malformed event is dropped at parse time — it can never wedge the consumer,
  but also never reaches the DLQ (accepted gap; the DLQ carries *send* failures,
  not parse failures).
- Both services ship the same Flyway `V1`; `flyway_schema_history` serialises
  the race when Argo starts them together (same pattern as flash-sale).
