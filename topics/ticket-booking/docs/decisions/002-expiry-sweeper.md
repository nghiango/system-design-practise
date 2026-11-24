# 002 — Hold expiry: lazy guards + a 10s DB sweeper, filter-on-read

## Context

A hold must expire ~5 minutes after it is taken, without the client
cooperating. Expiry has three consumers with different needs: the *write* paths
(hold/confirm must reclaim or refuse expired holds race-safely), the *read*
path (the seat map should not show a just-expired seat as sold), and the *audit*
path (someone must emit the `EXPIRED` event and reconcile the stored row).

## Options

- **Lazy expiry + scheduled sweeper** — every guarded UPDATE treats
  `status='HELD' AND hold_expires_at<now()` as free inside its WHERE clause, so
  expiry is *always* enforced at the moment it matters. A `@Scheduled` worker
  runs `UPDATE ... WHERE status='HELD' AND hold_expires_at<now() RETURNING
  event_id, seat, hold_owner` every 10s purely to reconcile storage and emit
  `EXPIRED`. Chosen.
- **Redis TTL keys + keyspace notifications** — put `hold:{seat}` in Redis with
  a TTL and consume expiry events. Keyspace notifications are pub/sub (at-most-
  once): a worker restart loses expiries, and while Redis is down nothing
  expires even though Postgres still says HELD. Two authorities for hold state
  must be kept consistent. Rejected.
- **Kafka delayed / retry topic** — publish `hold-expiry` events to be consumed
  after a delay. Broker delay is coarse and one-directional: an early release
  or a successful confirm can't un-delay the event, so the consumer must
  re-validate against Postgres anyway — the guarded UPDATE again, plus extra
  topic machinery. Rejected.
- **Strict read consistency** (seat map shows stored status) — simplest query,
  but a seat that expired 1s ago displays as sold for up to the sweep interval,
  costing real sales during an on-sale. Rejected: `filter-on-read` costs one
  timestamp comparison per row and is always honest.

## Decision

- Write correctness never depends on the sweeper: `hold`, `confirm`, `release`,
  and the decline path all carry `hold_expires_at` in their guards.
- The sweeper (`sd.sweeper.delay-ms`, default 10s) is reconciliation only —
  its UPDATE is the same guard as everything else, so racing a hold/confirm is
  safe; `RETURNING` yields exactly the freed seats for `EXPIRED` events.
- The seat map filters expired holds to `FREE` at read time (documented
  eventual consistency between stored row and response).

## Consequences

- Holds expire correctly even with the worker down; a crashed sweeper only
  delays `EXPIRED` audit events, never correctness.
- One partial index (`WHERE status='HELD'`) keeps each sweep O(expired) rather
  than O(venue).
- The EXPIRED publish is best-effort: a lost event thins the audit trail but
  the freed row is durable. Tightening that (outbox) is a future trade-off, not
  a correctness bug.
