# 001 — Atomic stock gate: Redis Lua check-and-decrement

## Context

A flash sale fires thousands of concurrent buys at a handful of items. The
check-then-decrement must be atomic — any read-modify-write race oversells —
and fast enough to give buyers an instant yes/no.

## Options

- **Redis Lua `GET`+`DECR` in one script** — a single round trip executes
  atomically on Redis's single-threaded command loop. O(1) per attempt,
  no lock held across a network hop, no retry loop. The counter itself is
  the gate: `stock:<itemId>` seeded at item creation.
- **Postgres row lock** (`SELECT ... FOR UPDATE` then decrement) — correct but
  serialises the entire burst on one row; every buyer queues behind a DB
  transaction. At flash-sale burst rates the DB becomes the outage.
  Rejected for the hot path.
- **Optimistic UPDATE** (`UPDATE items SET stock=stock-1 WHERE stock>0`, retry on
  conflict) — atomic in Postgres but pushes the contention to the DB and
  gives buyers no instant answer under load. Rejected for the hot path;
  the same guarded UPDATE is reused by order-worker as the drift detector.
- **Queue-only** (enqueue every attempt, worker decides) — no race at all but
  buyers get no synchronous sold-out answer and the queue absorbs the full
  burst. Rejected; a queue still sits behind the gate for *accepted* orders.

## Decision

`seckill-api` runs one Lua script per buy on `flash-sale:stock:<itemId>`:

```lua
local s = tonumber(redis.call('GET', KEYS[1]) or '-1')
if s == -1 then return -1 end
if s > 0 then redis.call('DECR', KEYS[1]); return 1 end
return 0
```

`-1` distinguishes a missing key from sold-out. On missing, the API seeds the
counter once from Postgres `items.stock` and retries the script once — a
flushed cache must not kill a live sale (an item absent from Postgres → 404).

**Fail closed**: any Redis error → 503. Never fall back to a racy DB check in
the hot path and never guess. Overselling is a business-correctness failure;
a 503 is a retryable annoyance.

**Kafka publish failure after a successful decrement**: the reserved unit is
returned (`INCR`) before answering 503, so a broker outage does not silently
shrink sellable stock.

## Consequences

- Redis becomes part of the correctness story: a Redis outage pauses sales
  (503) rather than risking oversell. Accepted trade-off, explicit in README §8.
- A single hot key means all buys for one item serialise on one Redis slot —
  fine, that is exactly the atomicity we want, at Redis throughput.
- The Redis counter is a *gate*, not the record: Postgres `orders`/`items`
  remain the durable truth and order-worker reconciles drift (002).
- Reseed-on-miss trusts `items.stock` for recovery; if Postgres stock itself
  lagged, the worker's guarded UPDATE + compensation bounds the error.
