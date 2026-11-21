# URL Shortener topic plan (approved choices 2026-11-21)

- ID generation: Postgres sequence + Base62 (rejected: hash truncation = collision handling; UUID = not short; KGS/Snowflake = extra coordination for no benefit at this scale).
- Services: `link-api` (POST /api/links, GET /api/links/{code}/stats; owns `links`, `link_clicks` tables; consumes `url-shortener.link.clicked` to count clicks) and `redirect` (GET /{code}: Redis GET → 302; miss → Postgres lookup → cache → 302; publishes click event to Kafka).
- Data: `links(id bigint pk default nextval('link_id_seq'), code text unique, url text, created_at)`; `link_clicks(code text pk, clicks bigint)` — consumer upserts counter.
- Redis: cache-aside key `url-shortener:link:<code>` → url, TTL 1h; negative-cache "missing" sentinels 60s to stop misses hammering the DB.
- Schema: db-init job creates DB; Flyway migrations in link-api create tables (redirect is read-only).
- Load evidence: load/k6 scripts + docs/README explaining capacity estimate, choices, rejected alternatives.
