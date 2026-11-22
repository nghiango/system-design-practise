# 002 — Click counting: Kafka events + upsert counter

## Context

Every redirect should be counted, but the count must never slow down or break the
redirect itself (p95 matters, analytics lag does not).

## Options

- **Kafka event per click, consumer upserts `link_clicks`** — redirect publishes
  `{"code": "..."}` and returns; `link-api` consumes with
  `INSERT ... ON CONFLICT (code) DO UPDATE SET clicks = clicks + 1`.
- **Synchronous `UPDATE links SET clicks=clicks+1`** in the redirect path — couples
  read latency to the write database and turns Postgres write outages into 5xx on
  redirects. Rejected.
- **Redis INCR flushed periodically** — fast, but adds a flush job and a second
  counter store to keep consistent. Rejected as more moving parts.
- **Event sourcing (store every click row)** — precise and replayable, but the table
  grows at redirect rate and stats reads need aggregation. A counter row per code is
  enough for this exercise. Rejected.

## Decision

`redirect` publishes to `url-shortener.link.clicked` (6 partitions, keyed by code).
Publish failures are logged at warn and swallowed — a dead broker must never break a
302. `link-api` consumes in group `url-shortener.click-counter` and upserts
`link_clicks`. Counts are therefore eventually consistent (seconds behind), which is
fine for a stats endpoint.

## Consequences

- At-least-once delivery can double-count on consumer retry — accepted; if exact
  counts matter later, add event ids and dedupe.
- The topic follows the `<topic>.<entity>.<event>` naming convention; other topics
  never produce to or consume from it.
