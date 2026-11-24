# Notification plan (2026-11-24)

- notify-api: POST /api/notifications {idempotencyKey, channel(email|sms), target, body} → INSERT into notifications (id bigserial, idempotency_key unique, channel, target, status QUEUED, attempts 0) ON CONFLICT (idempotency_key) DO NOTHING → 201 {id, status} (or existing id on duplicate = idempotent submit) → publish {id, attempt:0} to notification.requested. GET /api/notifications/{id} → status/attempts. Flyway owns schema (also in sender-worker per flash-sale pattern).
- sender-worker: consumes notification.requested AND notification.retry. Per send:
  1. rate limit: Redis Lua token bucket `rate:<channel>` (capacity 10/s, refill) → if no token, republish to notification.retry with attempt+1, NOT counted as failure.
  2. simulate channel send: deterministic fake — target containing "fail" → error; "slow" → transient. Real provider pluggable via interface.
  3. success → status SENT (UPDATE). transient failure → attempt<3 → publish retry attempt+1; else status DEAD + publish notification.dlq.
  Idempotent: send is keyed by notification id; status transitions only forward (QUEUED->SENDING->SENT|RETRY|DEAD).
- Load: load/notify.js k6 — N notifications, assert all SENT eventually, rate respected.
- Docs: README + decisions (retry topics vs sleep-backoff; Lua token bucket vs in-memory limiter; at-least-once+idempotency vs exactly-once claim).
