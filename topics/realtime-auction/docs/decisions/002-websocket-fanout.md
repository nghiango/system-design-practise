# 002 — WebSocket fan-out as a separate `auction-broadcast` service

## Decision

`auction-broadcast` is its own deployable: it consumes `realtime-auction.updates` (consumer group
`{prefix}broadcast`) and pushes each record to the WebSocket sessions registered under
`/ws/auctions/{auctionId}`. Sessions live in `ConcurrentHashMap<auctionId, CopyOnWriteArraySet>`
inside the process; connect/close/fan-out race safely, and a session that fails a send is dropped
rather than blocking the loop.

## Why

Watch traffic out-multiplies bid traffic by orders of magnitude (one accepted bid → up to 10⁵
pushes). It is pure read-side fan-out: no transaction, no consistency burden, embarrassingly
parallel per session. Keeping it off the bid path means a socket storm, slow clients, or a
broadcast replica dying can never delay or lose a bid.

## Rejected alternatives

- **WebSocket inside `auction-api`** — same-pod fan-out would couple watcher socket churn (slow
  TCP drains, half-open connections, per-message send loops) to the write path's threads, memory
  and connection pool. A degraded watcher experience would degrade bidding itself.
- **SSE (Server-Sent Events)** — workable for one-directional ticks and simpler transport, but
  each SSE stream is a long-lived HTTP/1.1 request on the standard servlet path, proxies buffer
  them unevenly, and browsers cap per-host connections; WebSocket is the standard mechanism for
  this traffic shape and its session registry is exactly the bookkeeping needed.
- **Long-poll** — re-establishes a request per tick; fine at low rates, wasteful and
  latency-quantised for live bids.
- **Broker-managed fan-out (e.g. STOMP over a shared broker)** — adds a second broker to the
  exercise for semantics Kafka+WebSocket already give; session bookkeeping still lands in the
  app.

## Consequences

- Scaling watchers = more `auction-broadcast` replicas; each holds only its own sessions and
  forwards the updates it consumes.
- Sessions are per-replica: a reconnect can land on a different replica — fine, because the Kafka
  stream (keyed, ordered per auction) is the shared source every replica reads.
- `/api/auctions/{id}/watchers` reports that replica's session count — a debug aid, not a global
  tally.
