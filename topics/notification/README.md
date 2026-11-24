# Notification System

Status: services implemented, load evidence pending · Focus: millions of messages, queues, retries, rate limits, delivery guarantees · Level: Intermediate

## 1. Problem and requirements

Functional:

- `POST /api/notifications {idempotencyKey, channel(email|sms), target, body}` → queues a
  notification; 201 on a fresh submit, 200 with the existing id on a duplicate (idempotent submit).
- `GET /api/notifications/{id}` (both services) → `{id, channel, status, attempts}`.
- Delivery is async: a worker consumes `notification.requested`/`notification.retry`, enforces a
  per-channel rate limit, sends, and drives `QUEUED → SENDING → SENT | QUEUED-retry | DEAD`.

Non-functional:

- Millions of messages: submit must be O(1) and the send path horizontally scalable.
- **Never double-send** on client retries, consumer redelivery, or late retry events.
- Per-channel provider quotas must be respected across all worker replicas.
- Transient provider failures retry with a bounded budget; permanent failures go to a DLQ.

## 2. Capacity estimation

| Dimension | Estimate | Basis |
|---|---|---|
| Submit rate | ~1–5k rps | one INSERT + one Kafka produce per call, both O(1) |
| Send rate | 10 msgs/s per channel | token bucket `rate:<channel>` (capacity 10, refill 10/s) |
| Queue depth | unbounded backlog | Kafka requested+retry topics absorb bursts above the send rate |
| Retry traffic | ≤ ~10% of sends | transient share of provider outcomes |
| Row size | ~200 B/notification | millions of rows ≈ hundreds of MB — fits one Postgres table |

At "millions of messages" scale the bottleneck is the *provider quota*, not the pipeline — the
bucket caps throughput by design and the queue smooths the burst. Scaling beyond one channel's
quota means more channels/providers or a higher refill rate, not more workers.

## 3. API

```
POST notify-api:8080/api/notifications
  {"idempotencyKey":"k-1","channel":"email","target":"u@x.com","body":"hi"}
  → 201 {"id":1,"status":"QUEUED"} | 200 {"id":1,"status":<current>} on duplicate
  → 400 bad channel/fields | 503 queue unavailable
GET  {notify-api,sender-worker}:8080/api/notifications/{id}
  → 200 {"id","channel","status","attempts"} | 404
Kafka notification.requested / .retry   {"id":1,"attempt":0}   (6 partitions, key = id)
Kafka notification.dlq                  {"id":1,"attempt":2,"reason":"..."}  (3 partitions)
```

## 4. Data model

Postgres `notification` database — Flyway `V1` shipped in **both** services (same migration set;
`flyway_schema_history` locking serialises concurrent application at parallel startup, same pattern
as flash-sale):

- `notifications(id bigserial pk, idempotency_key text unique, channel text, target text, body
  text, status text default 'QUEUED', attempts int default 0, created_at, updated_at timestamptz)`

Redis: `notification:rate:<channel>` → token bucket hash `{tokens, ts}` (ts = last refill ms,
Redis `TIME`).
Kafka: `notification.requested`, `notification.retry` (6 partitions each), `notification.dlq` (3).

Status machine: `QUEUED → SENDING → SENT`; transient & attempts<3 → `SENDING → QUEUED`
(attempts+1) → republish retry; permanent or attempts exhausted → `DEAD` + DLQ. Every UPDATE is
guarded by the current status so terminal rows can never move backwards.

## 5. Architecture

```mermaid
flowchart LR
  client -->|POST /api/notifications| api[notify-api]
  api -->|INSERT ON CONFLICT| pg[(Postgres)]
  api -->|{id,attempt:0}| kafka[(Kafka requested)]
  kafka --> worker[sender-worker]
  retry[(Kafka retry)] --> worker
  worker -->|Lua token bucket| redis[(Redis)]
  worker -->|guarded UPDATEs| pg
  worker -->|requeue same attempt| retry
  worker -->|DEAD event| dlq[(Kafka dlq)]
```

Submit path: INSERT `ON CONFLICT (idempotency_key) DO NOTHING` → inserted: publish
`{id,attempt:0}` + 201; conflicted: return the stored row's `{id,status}` + 200 — the same submit
never queues two sends.
Send path per event: load row (skip terminal) → Lua token bucket → claim `SENDING` → send →
SENT / requeue attempt+1 / DEAD+DLQ. Throttled events requeue the *same* attempt — rate limiting
never burns the retry budget.

## 6. The hard problems and how they are solved

- **Duplicate submits must not double-send**: `idempotency_key` is UNIQUE; a retried POST hits the
  conflict path and returns the existing id without republishing (001). Evidence: the integration
  test asserts one row and exactly one Kafka event for two identical submits.
- **At-least-once delivery + late retries**: every status UPDATE is guarded (`WHERE status = ...`),
  so a stale `notification.retry` delivery against a SENT/DEAD row is a no-op, and the `SENDING`
  claim acts as a compare-and-swap between read and send (002).
- **Provider quota across replicas**: the token bucket lives in Redis as `{tokens, ts}` and a
  single Lua script refills from elapsed `TIME` and decrements atomically — no inter-replica
  coordination, no local clock skew (002).
- **Throttling is not a failure**: a starved bucket republishes the unchanged event to
  `notification.retry`; only a consumed provider call increments `attempts` (bounded at 3).
- **Retry without blocking the partition**: retries go through a Kafka topic, not `Thread.sleep`,
  so a throttled message never stalls the partition behind it and redelivery is broker-driven
  (001). Simplicity trade-off: requeues are immediate (tight loop) rather than delayed — acceptable
  for the exercise since the bucket refill bounds the spin to ~100ms cycles at 10/s.
- **Poison messages**: permanent provider errors skip retry entirely (straight to DEAD+DLQ); a
  malformed event fails parsing and is dropped — neither can wedge the consumer.

## 7. Technology choices

| Need | Chosen | Why | Rejected alternatives | Decision record |
|---|---|---|---|---|
| Retry mechanism | `notification.retry` Kafka topic | broker-driven redelivery; never blocks the consumer thread or the partition; survives restarts | `Thread.sleep` in consumer (stalls partition, loses on crash), delayed bucket per delay-step (extra topics/infra for the same effect), DB scheduler poll (extra job + polling load) | [001](docs/decisions/001-retry-topics.md) |
| Rate limiter | Redis Lua token bucket (capacity 10, refill 10/s) | atomic across replicas, O(1), one round trip | in-memory per-instance limiter (N replicas → N× quota), DB counter (write load on hot path), provider-side limits only (429 storms, no control) | [002](docs/decisions/002-rate-limit-and-delivery-guarantee.md) |
| Delivery guarantee | at-least-once + idempotent submit + guarded status machine | honest guarantee: dedupe at every hand-off (UNIQUE key, ON CONFLICT, guarded UPDATE) | "exactly-once" claim (Kafka EOS covers the broker hop, not DB+provider; still needs idempotency), at-most-once (lost notifications) | [002](docs/decisions/002-rate-limit-and-delivery-guarantee.md) |
| Idempotent submit | `idempotency_key` UNIQUE + `ON CONFLICT DO NOTHING` | one constraint, works under racing submits | app-level check-then-insert (race window), dedupe table (extra writes) | [001](docs/decisions/001-retry-topics.md) |
| Schema management | Same Flyway V1 in both services | parallel deploys race; flyway lock serialises safely | one-service-owns-schema ordering coupling, separate migration job (extra moving part) | README §4 |
| Provider | `ChannelSender` interface + deterministic simulator | real SMTP/SMS client implements the same contract; pipeline fully testable offline | hard-coded provider SDK (untestable, one channel) | README §5 |

## 8. Failure scenarios and recovery

| Failure | Behaviour | Recovery |
|---|---|---|
| Kafka down at submit | INSERT succeeds, publish fails → 503; client retries with same key → dedupes against stored row | Kafka back → retry submit republishes? (row stays QUEUED — needs client retry or a reaper; documented gap) |
| Redis down | `tryAcquire` fails throttled → events requeue to `notification.retry` | Redis back → bucket refills from stored `ts`, drain resumes |
| Postgres down | worker can't load/claim → send path stalls; API submits fail | Postgres back → consumers resume from offset |
| sender-worker down | events accumulate in requested/retry topics | restart → group drains backlog; guarded UPDATEs make redelivery safe |
| Duplicate event delivery | terminal rows ignored; non-terminal re-claimed via guarded SENDING UPDATE | none needed |
| Provider permanent error | DEAD + `notification.dlq` event immediately | operator inspects DLQ; fix target and resubmit with a new key |
| Worker crash mid-send | row left in `SENDING` — the only un-guarded stall; a real deployment adds a SENDING-timeout reaper | manual reset or reaper job (documented gap) |

## 9. Run and verify

```bash
scripts/topic.sh port-forward notification notify-api 18080
scripts/topic.sh port-forward notification sender-worker 18081
curl -X POST http://127.0.0.1:18080/api/notifications \
  -d '{"idempotencyKey":"k1","channel":"email","target":"u@x.com","body":"hi"}' \
  -H 'Content-Type: application/json'
curl http://127.0.0.1:18081/api/notifications/1
```

Load test: `k6 run -e NOTIFY_API=http://127.0.0.1:18080 -e SENDER_API=http://127.0.0.1:18081
topics/notification/load/notify.js` — submits 100 notifications, polls until all reach a terminal
state, asserts 100% SENT. Results: not yet recorded (pending deployment).

## 10. Experiments to try next

- Submit the same `idempotencyKey` from parallel VUs — confirm one row and one send.
- Point a target at `flaky-*`/`fail-*` and watch `attempts`/`DEAD` + the DLQ topic.
- Drop the refill rate to 1/s and measure drain time for a 100-message burst — the queue should
  absorb it while the bucket paces sends.
- Kill Redis mid-drain and observe requeue-spin (no sends) vs. silent sends — the fail-throttled
  choice in action.
