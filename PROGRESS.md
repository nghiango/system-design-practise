# Progress

Legend: ✔ done · ◐ in progress · blank = not started · – not applicable

| Topic                  | Design | Services | Tests | Chart | Deployed | Load evidence | Docs | Status      |
|------------------------|--------|----------|-------|-------|----------|---------------|------|-------------|
| Foundation             |   ✔    |    –     |   ✔   |   ✔   |    ✔     |      –        |  ✔   | done        |
| URL Shortener          |   ✔    |    ✔     |   ✔   |   ✔   |          |               |  ✔   | ◐ in progress |
| Flash Sale             |        |          |       |       |          |               |      | not started |
| Notification System    |        |          |       |       |          |               |      | not started |
| Ticket Booking         |        |          |       |       |          |               |      | not started |
| Banking Money Transfer |        |          |       |       |          |               |      | not started |
| Real-Time Auction      |        |          |       |       |          |               |      | not started |

**Next step:** URL Shortener plan (services, ID generation, load tests).

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
