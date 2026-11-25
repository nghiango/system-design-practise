# Real-Time Auction

Status: services implemented, deploy + load evidence pending · Focus: concurrent bids, ordering,
WebSocket broadcast, consistency · Level: Advanced

## 1. Problem and requirements

Run auctions where many bidders fight over a single item in real time. The rules that make it an
auction exercise rather than a CRUD one:

- **Bids have a single authoritative order** — when 100 bids hit one auction at the same instant,
  there is exactly one answer to "who bid what, in which sequence". Every accepted bid gets a
  per-auction `seq` stamped while the order is decided, contiguous `1..N`.
- **The highest bid is never wrong** — a bid wins only if it strictly beats the current highest
  (or the start price when no bids exist). Concurrent equal bids: the first through the door wins,
  every other is rejected — no lost update ever decides an auction.
- **Losing bids are on the record** — a rejected bid is still written (`accepted = false`) so the
  audit trail explains *why* it lost, not just that it did.
- **Deadlines are enforced** — the first bid after `ends_at` flips the auction to `CLOSED` and is
  refused (410); everything after it is refused too.
- **Watchers see the same order** — the Kafka event for an accepted bid is keyed by `auctionId`,
  so all events for one auction live on one partition, which is ordered: the broadcasters forward
  bids in `seq` order (decision 001).
- **Watching scales separately from bidding** — a dedicated `auction-broadcast` service owns the
  WebSocket fan-out, so read-side watch load never steals CPU from the write path (decision 002).

Functional:

- `POST /api/auctions {title, startPrice, endsInSec}` → 201 `{id}`; 400 bad input.
- `POST /api/auctions/{id}/bids {bidder, amount}` → 200 `{seq}` on accept; 400 bad input; 404
  unknown auction; 409 `{reason, minimum}` when the bid does not beat the current highest (still
  recorded, `accepted = false`); 410 when the auction is closed — either already `CLOSED` or past
  `ends_at` (which lazily closes it).
- `GET /api/auctions/{id}` → `{highestBid, highestBidder, bidCount, status, endsAt}`; 404.
- `GET /api/auctions/{id}/bids` → accepted bids ordered by `seq`; 404.
- `GET auction-broadcast:8080/api/auctions/{id}/watchers` → live session count (debug aid).
- `WS auction-broadcast:8080/ws/auctions/{id}` → streams every accepted bid as
  `{auctionId, amount, bidder, seq, endsAt}`.

## 2. Capacity estimation

| Dimension | Estimate | Basis |
|---|---|---|
| Bids per auction | ~100 rps peak | one txn = 1 row lock + 1 insert + 1 update on ONE hot row; the row lock is the ceiling, not CPU — ~10 ms/txn ⇒ single auction tops out near serialisation |
| Concurrent auctions | thousands | different rows ⇒ independent locks; scales with Postgres connections, partitions spread naturally |
| Watchers per auction | 10⁴–10⁵ | fan-out is one Kafka record → N `sendMessage` calls; bounded by broadcast-service sockets and NIC, not by the API |
| Broadcast fan-out | O(watchers) per bid | `CopyOnWriteArraySet` iteration; scale by running more broadcast replicas — each gets every message (own consumer group member) and forwards to its own sessions |
| bids table | 1 row per attempt | rejected bids included; index `(auction_id, accepted, seq)` keeps the accepted-history scan narrow |

The bottleneck is **the single-item write path**: N bidders on one auction *must* serialise —
that serialisation is not a limitation to engineer away, it IS the mechanism that produces the
order everyone agrees on.

## 3. API

```
POST auction-api:8080/api/auctions            {"title":"watch","startPrice":10.00,"endsInSec":300}
  → 201 {"id":1} | 400
POST auction-api:8080/api/auctions/1/bids     {"bidder":"alice","amount":42.00}
  → 200 {"seq":1}                               (accepted — new highest)
  → 409 {"reason":"bid must be higher than 42.00","minimum":42.00}   (recorded, accepted=false)
  → 410 {"error":"auction closed"|"auction ended"}                  (closed / past ends_at)
  → 400 amount ≤ 0 | missing bidder             → 404 unknown auction
GET  auction-api:8080/api/auctions/1          → 200 {highestBid, highestBidder, bidCount, status, endsAt}
GET  auction-api:8080/api/auctions/1/bids     → 200 [{bidder, amount, seq, at}]  (accepted, seq order)
WS   auction-broadcast:8080/ws/auctions/1     → text {"auctionId":1,"amount":42.00,"bidder":"alice","seq":1,"endsAt":"…"}
GET  auction-broadcast:8080/api/auctions/1/watchers → 200 {"auctionId":1,"watchers":3}
Kafka realtime-auction.updates (6 partitions, key = auctionId)
  {"auctionId":1,"amount":42.00,"bidder":"alice","seq":1,"endsAt":"…"}
```

## 4. Architecture

Two services, **one Postgres database** (`realtime_auction`) — the auction row is the single
sequencer (decision 001):

```
bidder ──POST /api/auctions/{id}/bids──▶ auction-api ──ONE txn──▶ Postgres
                                            │                    auctions / bids
                                            └── 1. SELECT ... FOR UPDATE  (single sequencer)
                                                2. CLOSED or now>ends_at → close if needed → 410
                                                3. amount ≤ COALESCE(highest,start) → bids(accepted=false) → 409
                                                4. else: seq=++bid_count, bids(accepted=true),
                                                   UPDATE auctions highest_* 
                                            │ commits
                                            └─ KafkaTemplate.send(key=auctionId, {seq,…})
                                                          │
Kafka realtime-auction.updates (key → partition ⇒ order)  │
                                                          ▼
watcher ──WS /ws/auctions/{id}──▶ auction-broadcast ──@KafkaListener──┘
                                     ConcurrentHashMap<auctionId, sessions>
                                     → sendMessage to each session; failures drop the session
```

Why these primitives:

- **`SELECT ... FOR UPDATE` on the auction row** — the database is the one place all bidders
  already meet. The lock serialises bids; each contender re-reads `highest_bid` *after* acquiring
  it, so the max-check is exact and the seq assignment is the lock-acquisition order — recorded
  durably, not just observed.
- **Rejected bids written (`accepted=false`)** — "why did I lose?" is answerable from data, and
  rejection is as durable as acceptance.
- **Kafka key = auctionId** — one key → one partition → total order per auction. The broadcasters
  never have to re-order; the transport preserves what the lock decided.
- **Separate `auction-broadcast` service** — watch traffic is read-side and hugely
  out-multiplies bid traffic (one bid → thousands of sends). Splitting it keeps slow clients and
  socket storms off the write path, and lets each side scale on its own axis.

### Alternatives considered (and rejected)

- **Kafka-ordering-only writes** (append bids to a topic, apply in consumer order) — moves the
  winner decision off the request path: the 200/409 response would need to wait for a projected
  result or lie. The row lock answers synchronously in one round-trip. Rejected; see decision 001.
- **Distributed lock (Redis `SET NX` / Redlock) per auction** — adds a lock manager whose lease
  expiry, GC pauses and fencing are new failure modes the DB row lock simply does not have; the
  `highest_bid` comparison would still need the DB. Rejected; see decision 001.
- **Optimistic check-then-update** — under READ COMMITTED two bidders read the same highest and
  both "win"; needs a retry loop or version column that re-serialises anyway. Rejected.
- **WebSocket inside auction-api** — a stuck watcher thread or socket backpressure would sit on
  the same JVM and connection pool as bidding. Rejected; see decision 002.
- **SSE / long-poll instead of WebSocket** — workable fan-out, but one-directional-only upgrades
  and proxy buffering make it a poor fit for "live tick" semantics; WebSocket is the standard
  answer and Spring's session registry is exactly the bookkeeping needed. Rejected; see 002.
- **`double`/`float` for money** — binary floats cannot represent 0.10; `numeric(19,2)` /
  `BigDecimal` throughout.

## 5. Data model

Flyway `V1` shipped in **both** services (same migration set; `flyway_schema_history` locking
serialises parallel startup, same pattern as the other topics):

- `auctions(id, title, start_price numeric(19,2) CHECK ≥ 0, highest_bid, highest_bidder,
  status OPEN|CLOSED, ends_at, bid_count DEFAULT 0, created_at)`
- `bids(id, auction_id → auctions, bidder, amount numeric(19,2) CHECK > 0, accepted, seq NULL,
  at)` — `seq` is set only for accepted bids and equals `bid_count` at that instant: seq order ==
  lock order == broadcast order. Index `(auction_id, accepted, seq)`.

Invariants: `bid_count == count(accepted bids)`, `highest_bid == max(accepted bid amounts)`,
`seq` is contiguous `1..bid_count` — all three are asserted by the concurrency evidence test.

## 6. Failure and recovery

| Failure | Behaviour |
|---|---|
| Crash mid-bid | whole txn rolls back; no partial seq, no phantom highest |
| Concurrent equal bids | row lock serialises; first wins, rest see the new highest → 409, each recorded rejected |
| Bid after `ends_at` | first arrival flips status to `CLOSED` and is refused; subsequent bids see `CLOSED` → 410 |
| Kafka publish fails after commit | the bid is safe in `bids`; watchers miss one tick and recover on the next — `GET /api/auctions/{id}` is the source of truth |
| Watcher socket dies mid-send | session is dropped (not the loop); other watchers unaffected |
| Broadcast replica ×N | each consumes the full stream (same group → partitioned; replicas share the load *per partition*: sessions on replica R get updates forwarded by R) |
| Malformed Kafka event | dropped, partition not wedged |

## 7. Observability

- Counters: `bids.accepted`, `bids.rejected`, `bids.closed` (auction-api).
- `GET /api/auctions/{id}/watchers` on auction-broadcast — live fan-out size.
- Actuator `/actuator/prometheus`, health probes on both services.

## 8. Decisions

- [001 — Bid ordering: Postgres row lock](docs/decisions/001-bid-ordering.md)
- [002 — WebSocket fan-out as a separate service](docs/decisions/002-websocket-fanout.md)

## 9. Run

```bash
./gradlew :realtime-auction:topicTest :realtime-auction:topicAssemble
# on the cluster (one topic at a time):
scripts/topic.sh activate realtime-auction
scripts/topic.sh port-forward realtime-auction auction-api 18080
scripts/topic.sh port-forward realtime-auction auction-broadcast 18081
k6 run -e AUCTION_API=http://127.0.0.1:18080 topics/realtime-auction/load/auction.js
```
