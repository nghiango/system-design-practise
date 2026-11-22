# 001 — Short code generation: Postgres sequence + Base62

## Context

Every link needs a short, unique, URL-safe code. ~100k links/day at target scale —
far below anything requiring distributed coordination.

## Options

- **Postgres sequence + Base62** — `id bigint default nextval('link_id_seq')`, code = base62(id).
  Short (7 chars covers 3.5 trillion), monotonically increasing, zero extra infrastructure.
- **Hash truncation (md5/sha of URL)** — needs collision handling on every insert and
  cannot serve "same URL, different code" semantics cleanly.
- **UUID** — 36 chars, not short; truncating it reintroduces collisions.
- **Key-generation service / Snowflake** — distributed ids buy coordination cost for
  uniqueness we already get for free from the database. Sharding later can switch to
  per-shard sequences with offset/stride without changing code format.

## Decision

Postgres sequence + Base62 encoding (`[a-zA-Z0-9]`). `link-api` selects
`nextval('link_id_seq')`, encodes it and inserts id+code+url in one transaction.

## Consequences

- Codes leak creation order and rate — acceptable for this exercise; mitigate later
  with offset/stride per shard if scraping becomes a concern.
- The sequence is the single writer bottleneck; at ~1.2 inserts/s it is irrelevant.
- Cache and redirect paths only see `code`; the numeric id never leaves link-api.
