# 002 — Order persistence: Kafka `order.created` events + idempotent worker

## Context

The Redis gate answers in milliseconds but does not persist anything. Accepted
orders must reach Postgres durably, survive retries, and keep the API fast.

## Options

- **Kafka event per accepted sale, worker writes `orders` + decrements
  `items.stock`** — the API publishes `{"orderId","itemId"}` and returns 202.
  `order-worker` consumes in group `flash-sale.order-worker` and applies the
  order. The topic volume is naturally bounded: only *accepted* sales produce
  events, so the gate caps Kafka at inventory size.
- **Synchronous `INSERT INTO orders` in the request path** — couples the 202
  latency to Postgres write health and turns DB write outages into failed
  sales after stock was already decremented. Rejected.
- **Redis as the order store** (e.g. per-item list) — fast but not the durable
  record this exercise wants in Postgres; adds a second drain job. Rejected.

## Decision

- `seckill-api` publishes to `flash-sale.order.created` (6 partitions, key =
  `orderId`) **synchronously** — unlike url-shortener click events, a failed
  publish is not swallowed: the API INCRs the unit back and returns 503.
- `order-worker` persists in two steps:
  1. `INSERT INTO orders ... ON CONFLICT (order_id) DO NOTHING` — `order_id`
     as PK makes at-least-once delivery idempotent; a redelivery returns 0
     rows and stops before touching stock.
  2. `UPDATE items SET stock = stock - 1 WHERE id = ? AND stock > 0` — the
     guarded decrement doubles as the drift detector between the Redis
     counter and Postgres.
- **Compensation on drift**: if step 2 affects 0 rows, Postgres already treats
  the item as sold out. The worker marks the order `failed` and INCRs the
  Redis counter back — saga-style compensation, bounded and explicit, logged
  for reconciliation.
- Both services ship the same Flyway `V1` migration (items, orders); the
  `flyway_schema_history` lock serialises concurrent application when Argo
  starts both at once.

## Consequences

- Orders are eventually consistent: `GET /api/orders/{orderId}` may 404 for a
  few hundred ms after a 202. Acceptable; the gate already told the buyer yes.
- A `failed` order after a 202 means drift occurred — the client may need
  to handle "accepted then failed" (e.g. refund path in a real system).
- The topic follows `<topic>.<entity>.<event>`; other topics never produce to
  or consume from it.
