# Real-Time Auction plan (2026-11-24)

- auction-api: Flyway V1 (both services): auctions(id bigserial pk, title, start_price numeric(19,2), highest_bid numeric(19,2), highest_bidder text, status OPEN|CLOSED, ends_at timestamptz, bid_count int default 0); bids(id bigserial pk, auction_id, bidder, amount, accepted bool, seq int, at).
  - POST /api/auctions {title, startPrice, endsInSec} -> 201.
  - POST /api/auctions/{id}/bids {bidder, amount} -> ONE txn: SELECT auction FOR UPDATE; closed or now>ends_at -> 410 (auto-close status). amount <= COALESCE(highest_bid, start_price) -> record bids(accepted=false) -> 409. Else accept: insert bid accepted=true seq=++bid_count, update auction highest* -> 200 {seq} + publish realtime-auction.updates {auctionId, amount, bidder, seq} (keyed by auctionId => Kafka partition order = bid order).
  - GET /api/auctions/{id} -> {highestBid, highestBidder, bidCount, status, endsAt}; GET bids.
  - Concurrent equal bids: row lock serializes; first wins, second sees updated highest -> 409. Evidence test.
- auction-broadcast: Spring WebSocket (spring-boot-starter-websocket) at /ws/auctions/{id}; @KafkaListener realtime-auction.updates group ${SD_KAFKA_GROUP_PREFIX}broadcast -> broadcast JSON to sessions of that auction. Doc: split broadcast from api so API scales independently; per-auction session map; cleanup on close.
- k6 load/auction.js: create auction, 500 VUs bid increasing prices distinct users, assert highest==max bid, bidCount==attempts-1... (each VU bids once with random amount -> assert highest bid == max amount sent, accepted count == bidCount).
- Docs README + decisions/001-row-ordering-vs-lock.md (FOR UPDATE vs Redis vs Kafka-first), 002-ws-fanout.md (separate broadcaster vs in-pod broadcast).
