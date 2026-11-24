# Progress

Legend: ✔ done · ◐ in progress · blank = not started · – not applicable

| Topic                  | Design | Services | Tests | Chart | Deployed | Load evidence | Docs | Status      |
|------------------------|--------|----------|-------|-------|----------|---------------|------|-------------|
| Foundation             |   ✔    |    –     |   ✔   |   ✔   |    ✔     |      –        |  ✔   | done        |
| URL Shortener          |   ✔    |    ✔     |   ✔   |   ✔   |          |               |  ✔   | ◐ in progress |
| Flash Sale             |   ✔    |    ✔     |   ✔   |   ✔   |          |               |  ✔   | ◐ in progress |
| Notification System    |   ✔    |    ✔     |   ✔   |   ✔   |          |               |  ✔   | ◐ in progress |
| Ticket Booking         |   ✔    |    ✔     |   ✔   |   ✔   |          |               |  ✔   | ◐ in progress |
| Banking Money Transfer |        |          |       |       |          |               |      | not started |
| Real-Time Auction      |        |          |       |       |          |               |      | not started |

**Next step:** run `topics/url-shortener/load/links.js` (k6) for load evidence, then the next topic.

## Foundation checklist
Plan: [docs/superpowers/plans/2026-11-18-foundation.md](docs/superpowers/plans/2026-11-18-foundation.md)

- [x] 1. Gradle monorepo, `sd.spring-service` convention, tracker and docs skeleton
- [x] 2. Generic topic Helm chart
- [x] 3. GitOps publish script with safe push
- [x] 4. Topic switch script
- [x] 5. Jenkins pipelines and job registration
- [x] 6. GitOps ApplicationSet and `topics/active.yaml` 
- [x] 7. `scripts/topic.sh` operator helper
- [x] 8. Platform change: Prometheus/Alloy watch topic namespaces (hyper-scale-commerce ADR-0032)
- [x] 9. End-to-end verification on the cluster

## Log
- 2026-11-18 — Design approved; foundation plan written.
- 2026-11-18 — Task 1: Gradle monorepo with topic discovery and the `sd.spring-service` convention.
- 2026-11-18 — Task 2: generic topic chart (`deploy/topic-chart`) with exclusivity wait, DB/Kafka bootstrap and network policies.
- 2026-11-18 — Task 3: `ci/scripts/publish_topic.py` publishes one topic folder to GitOps, with rebase-and-retry push.
- 2026-11-18 — Task 4: `ci/scripts/topic_switch.py` (activate / deactivate / status) writes the single `topics/active.yaml` value.
- 2026-11-18 — Task 5: `ci/Jenkinsfile` (one job per topic), `ci/Jenkinsfile.switch` and `ci/scripts/register_jenkins_jobs.py`.
- 2026-11-18 — Task 6: ApplicationSet `system-design-topics` and `topics/active.yaml` committed in the GitOps repo (branch `system-design-topics`).
- 2026-11-18 — Task 7: `scripts/topic.sh` (status, port-forward, logs, guarded reset).
- 2026-11-19 — Task 8: hyper-scale-commerce `1e689e9` — read-only ClusterRoles `local-monitoring-topics` / `local-log-reader-topics`, Prometheus `topics` job, Alloy `topic_pods`, "System Design Topics" dashboard (ADR-0032). Platform release: Jenkins `hyperscale-local-ci` #9.
- 2026-11-19 — Task 9 evidence (kind-hyperscale-k8s):
  - Jenkins `system-design/url-shortener` #3 published `44ef343`; `topic-switch` #1 activate → Argo `topic-url-shortener` Synced/Healthy.
  - Hook jobs: `wait-for-exclusive` "no other topic online", `db-init` "CREATE DATABASE", `kafka-topics` created `url-shortener.link.clicked` (6 partitions, RF 3, min ISR 2). `url_shortener` present on primary and both replicas.
  - Exclusivity: with a labelled `topic-dummy` namespace present, the wait job logged "still online: topic-dummy" and db-init/kafka jobs did not start; after deleting it the sync completed. Re-run `db-init` was idempotent.
  - Observability: Loki returns topic job logs with `topic="url-shortener"`; Prometheus has the `topics` job loaded (no targets yet: no services); existing targets 4/4 up. RBAC: no secrets access, no write verbs.
  - `topic-switch` #2 deactivate → Application and namespace removed in ~110 s; shared-namespace policies and topic ClusterRoles removed; database and Kafka topic kept.
  - Bugs found and fixed (`44ef343`): wait script did not match pretty-printed API JSON; first build of a new topic job had no `IMAGE_PREFIX`.
- 2026-11-21 — URL Shortener services: `link-api` (POST/GET /api/links, Flyway V1 schema, click-counter Kafka consumer, `links.created` metric) and `redirect` (GET /{code} cache-aside via Redis with 1h url / 60s NOTFOUND sentinel, best-effort click event to `url-shortener.link.clicked`). Postgres sequence + Base62 codes (decision 001); Kafka upsert counting (decision 002). Unit + Testcontainers integration tests, k6 script, ARM64 Dockerfiles.
- 2026-11-24 — URL Shortener services live: link-api + redirect deployed via Jenkins #9 / Argo `7720786`.
  Verified end-to-end: POST /api/links (Base62 code), GET /{code} → 302, Kafka click events → `link_clicks`
  counter (3 clicks counted), Prometheus `topics` targets up with `topic="url-shortener"`, logs in Loki.
  Fixes during rollout: actuator prometheus endpoint needed `management.endpoints.web.exposure.include` (bbc38c8).
  Remaining for "done": k6 load evidence (`topics/url-shortener/load/links.js`).
- 2026-11-24 — Flash Sale services: `seckill-api` (POST /api/items preloads Redis `stock:<id>`, GET /api/items
  with live stock, POST /api/sales gated by an atomic Redis Lua check+decrement — 202/409/404/503 fail-closed,
  `sales.accepted`/`sales.rejected` metrics, Kafka publish compensation INCR) and `order-worker` (@KafkaListener
  `flash-sale.order.created` group `flash-sale.order-worker`, idempotent INSERT ON CONFLICT, guarded stock UPDATE,
  drift compensation marks order failed + INCR back, GET /api/orders/{orderId}). Redis Lua gate (decision 001);
  Kafka order events + idempotent consumer (decision 002). Unit + Testcontainers integration tests (200 racing
  buyers on stock=100 → exactly 100 accepted), k6 oversell script, ARM64 Dockerfiles.
  Remaining for "done": deploy + k6 load evidence (`topics/flash-sale/load/seckill.js`).
- 2026-11-24 — Flash Sale live: seckill-api + order-worker via Jenkins #1 / topic-switch #4.
  Exclusivity verified live: flash-sale wait job held until topic-url-shortener was fully removed (~60s),
  then db-init/kafka/services started. url-shortener's database preserved after switch-off.
  Seckill evidence: stock=3 → 3×202 accepted + 1×409 sold out; orders persisted idempotently via Kafka.
  Integration test: 200 threads on stock=100 → exactly 100 accepted. Prometheus topics targets up.
  Remaining for "done": k6 oversell evidence (topics/flash-sale/load/seckill.js).
- 2026-11-24 — Notification services: `notify-api` (POST /api/notifications idempotent submit via
  UNIQUE idempotency_key + INSERT ON CONFLICT DO NOTHING — 201 fresh / 200 existing, publish
  {id,attempt:0} to `notification.requested`, GET /api/notifications/{id},
  `notifications.submitted`/`notifications.deduplicated` metrics) and `sender-worker`
  (@KafkaListener on `notification.requested`+`notification.retry` group `<prefix>sender`, Redis Lua
  token bucket `rate:<channel>` 10/s — throttled requeues same attempt, guarded status machine
  QUEUED→SENDING→SENT / retry attempts+1 / DEAD+`notification.dlq` at 3rd attempt or permanent
  error, `notifications.sent`/`dead`/`retried` metrics, same GET). Retry topics (decision 001);
  Lua token bucket + at-least-once/idempotent delivery (decision 002). Deterministic
  ChannelSender simulator (fail→PERMANENT, flaky→TRANSIENT then success). Flyway V1 in both
  services. Unit + Testcontainers integration tests (22 notifications → all terminal, dup submit
  → same id + one event), k6 submit-and-poll script, ARM64 Dockerfiles.
  Remaining for "done": deploy + k6 load evidence (`topics/notification/load/notify.js`).
- 2026-11-24 — Notification live: notify-api + sender-worker via Jenkins #1 / topic-switch #5.
  Verified: normal -> SENT (0 attempts), flaky -> SENT (1 attempt via notification.retry),
  permanent-fail -> DEAD, duplicate idempotency key -> same id (no double-send).
  Redis Lua token bucket (10/s/channel, Redis TIME refill), guarded status machine, DLQ topic.
  Prometheus targets up. Remaining for "done": k6 load evidence (topics/notification/load/notify.js).
- 2026-11-24 — Ticket Booking services: booking-api (atomic guarded-UPDATE holds with 5-min TTL,
  transactional confirm with simulated decline payment, owner-guarded release, filter-on-read seat
  map) and booking-worker (10s UPDATE...RETURNING sweeper, booking.events → booking_history audit
  consumer, history API). Postgres row locking (decision 001); lazy guards + sweeper (decision 002).
  Unit + Testcontainers tests (100-thread same-seat race → exactly 1 winner), k6 book.js,
  ARM64 Dockerfiles. Remaining for "done": deploy + k6 load evidence.
- 2026-11-24 — Ticket Booking live: booking-api + booking-worker via Jenkins #1 / topic-switch #6.
  Verified: atomic hold (alice 200, bob 409 same seat), confirm->BOOKED+PAID, declined payment 402
  frees the seat (re-hold 200), HELD/CONFIRMED/DECLINED events audited via Kafka booking_history.
  Integration: 100 threads hold same seat -> exactly 1 wins; sweeper frees expired holds.
  Remaining for "done": k6 load evidence (topics/ticket-booking/load/book.js).
