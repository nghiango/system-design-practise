# Progress

Legend: ✔ done · ◐ in progress · blank = not started · – not applicable

| Topic                  | Design | Services | Tests | Chart | Deployed | Load evidence | Docs | Status      |
|------------------------|--------|----------|-------|-------|----------|---------------|------|-------------|
| Foundation             |   ✔    |    –     |   ◐   |   ✔   |          |      –        |  ◐   | in progress |
| URL Shortener          |        |          |       |       |          |               |      | not started |
| Flash Sale             |        |          |       |       |          |               |      | not started |
| Notification System    |        |          |       |       |          |               |      | not started |
| Ticket Booking         |        |          |       |       |          |               |      | not started |
| Banking Money Transfer |        |          |       |       |          |               |      | not started |
| Real-Time Auction      |        |          |       |       |          |               |      | not started |

**Next step:** Foundation Task 4, the topic switch script.

## Foundation checklist
Plan: [docs/superpowers/plans/2026-11-18-foundation.md](docs/superpowers/plans/2026-11-18-foundation.md)

- [x] 1. Gradle monorepo, `sd.spring-service` convention, tracker and docs skeleton
- [x] 2. Generic topic Helm chart
- [x] 3. GitOps publish script with safe push
- [ ] 4. Topic switch script
- [ ] 5. Jenkins pipelines and job registration
- [ ] 6. GitOps ApplicationSet and `topics/active.yaml`
- [ ] 7. `scripts/topic.sh` operator helper
- [ ] 8. Platform change: Prometheus/Alloy watch topic namespaces
- [ ] 9. End-to-end verification on the cluster

## Log
- 2026-11-18 — Design approved; foundation plan written.
- 2026-11-18 — Task 1: Gradle monorepo with topic discovery and the `sd.spring-service` convention.
- 2026-11-18 — Task 2: generic topic chart (`deploy/topic-chart`) with exclusivity wait, DB/Kafka bootstrap and network policies.
- 2026-11-18 — Task 3: `ci/scripts/publish_topic.py` publishes one topic folder to GitOps, with rebase-and-retry push.
