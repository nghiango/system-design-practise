# System Design Practise

Complete, working Java reference solutions for six system-design topics. Each
topic can be deployed to the local kind cluster (`kind-hyperscale-k8s`) through
GitOps, and at most one topic is online at a time.

| Topic | Focus | Level |
|---|---|---|
| URL Shortener | capacity estimation, sharding, caching, ID generation | Foundational |
| Flash Sale | inventory consistency, high concurrency, overselling prevention | Intermediate–Advanced |
| Notification System | high-volume queues, retries, rate limits, delivery guarantees | Intermediate |
| Ticket Booking | seat locking, reservation expiry, payment, race conditions | Advanced |
| Banking Money Transfer | ACID, idempotency, ledger, distributed transactions, recovery | Advanced |
| Real-Time Auction | concurrent bids, ordering, WebSocket broadcast, consistency | Advanced |

- Progress: [PROGRESS.md](PROGRESS.md)
- Design: [docs/superpowers/specs/2026-11-18-system-design-practise-design.md](docs/superpowers/specs/2026-11-18-system-design-practise-design.md)
- Each topic's own doc: `topics/<topic>/README.md` (template: [docs/topic-template.md](docs/topic-template.md))

## Which part belongs to which topic

Every artefact carries the topic slug (`url-shortener`, `flash-sale`, `notification`,
`ticket-booking`, `banking-transfer`, `auction`):

| Artefact | Pattern | Example |
|---|---|---|
| Source folder | `topics/<topic>/` | `topics/flash-sale/` |
| Gradle module | `:<topic>:<service>` | `:flash-sale:inventory-service` |
| Java package | `com.systemdesign.<topicnodash>.<service>` | `com.systemdesign.flashsale.inventory` |
| Container image | `<registry>/system-design/<topic>-<service>:<sha>` | `.../system-design/flash-sale-inventory-service:ab12…` |
| GitOps folder | `topics/<topic>/` (in `hyper-scale-commerce-gitops`) | `topics/flash-sale/` |
| Argo Application | `topic-<topic>` | `topic-flash-sale` |
| Namespace | `topic-<topic>` | `topic-flash-sale` |
| Label on every resource | `system-design/topic: <topic>` | `system-design/topic: flash-sale` |
| Postgres database | `<topic_underscored>` | `flash_sale` |
| Kafka topic | `<topic>.<entity>.<event>` | `flash-sale.order.created` |
| Kafka consumer group | `<topic>.<service>` | `flash-sale.order-service` |
| Redis key prefix | `<topic>:` | `flash-sale:stock:{sku}` |
| Jenkins job | `system-design/<topic>` | `system-design/flash-sale` |
| Logs (Loki) | `{namespace="topic-<topic>"}` | |

## Layout

```
build-logic/          Gradle convention plugin `sd.spring-service` shared by every service
deploy/topic-chart/   the one Helm chart every topic is deployed with
ci/                   Jenkins pipelines and the scripts they run
scripts/topic.sh      status, port-forward, logs, reset for a topic
topics/<topic>/       topic.yaml, README.md, docs/decisions/, services/, deploy/values.yaml, load/
```

## Build and test

```bash
./gradlew build                          # everything
./gradlew :url-shortener:topicTest       # one topic's unit tests
./gradlew -p build-logic test            # the build convention itself
```
