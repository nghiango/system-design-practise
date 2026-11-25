package com.systemdesign.realtimeauction.auctionapi;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Bidding as a single Postgres transaction (decision 001).
 *
 * <p>{@link #placeBid} is ONE {@code @Transactional} unit: {@code SELECT ... FOR UPDATE} on the
 * auction row makes the database the single sequencer — concurrent bids queue on the row lock and
 * each one sees the winner's updated {@code highest_bid} before deciding. There is no lock manager
 * to lose, no ordering channel to drift out of sync: the order bids acquire the lock IS the order
 * they are assigned, and {@code seq = bid_count + 1} stamps that order durably.
 */
@Service
class AuctionService {

  private final JdbcClient jdbc;
  private final BidEventPublisher publisher;

  AuctionService(JdbcClient jdbc, BidEventPublisher publisher) {
    this.jdbc = jdbc;
    this.publisher = publisher;
  }

  enum BidResult {
    ACCEPTED,
    REJECTED,
    CLOSED,
    NOT_FOUND
  }

  record BidOutcome(BidResult result, Integer seq, BigDecimal minimum, String reason) {}

  /** The locked auction row as the winning bid sees it. */
  record LockedAuction(
      String status,
      BigDecimal startPrice,
      BigDecimal highestBid,
      String highestBidder,
      OffsetDateTime endsAt,
      int bidCount) {}

  record AuctionView(
      long id,
      String title,
      BigDecimal startPrice,
      BigDecimal highestBid,
      String highestBidder,
      String status,
      OffsetDateTime endsAt,
      int bidCount) {}

  record BidView(
      long id, long auctionId, String bidder, BigDecimal amount, Integer seq, OffsetDateTime at) {}

  // ---- auctions ---------------------------------------------------------------------------

  @Transactional
  long createAuction(String title, BigDecimal startPrice, long endsInSec) {
    return jdbc.sql(
            "INSERT INTO auctions (title, start_price, ends_at) VALUES (:title, :startPrice, now()"
                + " + make_interval(secs => :secs)) RETURNING id")
        .param("title", title)
        .param("startPrice", startPrice)
        .param("secs", endsInSec)
        .query(Long.class)
        .single();
  }

  Optional<AuctionView> auction(long id) {
    return jdbc.sql(
            "SELECT id, title, start_price, highest_bid, highest_bidder, status, ends_at,"
                + " bid_count FROM auctions WHERE id = :id")
        .param("id", id)
        .query(
            (rs, rowNum) ->
                new AuctionView(
                    rs.getLong("id"),
                    rs.getString("title"),
                    rs.getBigDecimal("start_price"),
                    rs.getBigDecimal("highest_bid"),
                    rs.getString("highest_bidder"),
                    rs.getString("status"),
                    rs.getObject("ends_at", OffsetDateTime.class),
                    rs.getInt("bid_count")))
        .optional();
  }

  boolean auctionExists(long id) {
    return jdbc.sql("SELECT 1 FROM auctions WHERE id = :id")
        .param("id", id)
        .query(Integer.class)
        .optional()
        .isPresent();
  }

  /** Accepted bids in bid order — the auction's history as watchers should have seen it. */
  List<BidView> acceptedBids(long auctionId) {
    return jdbc.sql(
            "SELECT id, auction_id, bidder, amount, seq, at FROM bids"
                + " WHERE auction_id = :id AND accepted ORDER BY seq")
        .param("id", auctionId)
        .query(
            (rs, rowNum) ->
                new BidView(
                    rs.getLong("id"),
                    rs.getLong("auction_id"),
                    rs.getString("bidder"),
                    rs.getBigDecimal("amount"),
                    rs.getInt("seq"),
                    rs.getObject("at", OffsetDateTime.class)))
        .list();
  }

  // ---- bids ---------------------------------------------------------------------------------

  /**
   * Places a bid in one transaction.
   *
   * <ol>
   *   <li>{@code SELECT ... FOR UPDATE} on the auction row — the single sequencer. While the lock
   *       is held no other bid can interleave, so the highest-bid comparison below is exact.
   *   <li>Missing row → NOT_FOUND. {@code status = CLOSED}, or {@code now > ends_at} (which lazily
   *       flips the row to CLOSED — the first bid after the deadline is the closing event) →
   *       CLOSED, both → 410.
   *   <li>{@code amount <= COALESCE(highest_bid, start_price)} → a {@code bids} row with {@code
   *       accepted = false} is still written: a rejection is part of the auction's record, so a
   *       losing bidder can always see *why* they lost (decision 001).
   *   <li>Otherwise the bid wins: {@code seq = bid_count + 1} makes the lock-acquisition order
   *       durable, the auction row moves to the new highest, and the update is published to {@code
   *       realtime-auction.updates} keyed by auction id.
   * </ol>
   */
  @Transactional
  BidOutcome placeBid(long auctionId, String bidder, BigDecimal amount) {
    Optional<LockedAuction> locked =
        jdbc.sql(
                "SELECT status, start_price, highest_bid, highest_bidder, ends_at, bid_count"
                    + " FROM auctions WHERE id = :id FOR UPDATE")
            .param("id", auctionId)
            .query(
                (rs, rowNum) ->
                    new LockedAuction(
                        rs.getString("status"),
                        rs.getBigDecimal("start_price"),
                        rs.getBigDecimal("highest_bid"),
                        rs.getString("highest_bidder"),
                        rs.getObject("ends_at", OffsetDateTime.class),
                        rs.getInt("bid_count")))
            .optional();
    if (locked.isEmpty()) {
      return new BidOutcome(BidResult.NOT_FOUND, null, null, "unknown auction " + auctionId);
    }
    LockedAuction auction = locked.get();

    if ("CLOSED".equals(auction.status())) {
      return new BidOutcome(BidResult.CLOSED, null, null, "auction closed");
    }
    if (OffsetDateTime.now().isAfter(auction.endsAt())) {
      // First arrival past the deadline closes the auction for everyone after it.
      jdbc.sql("UPDATE auctions SET status = 'CLOSED' WHERE id = :id")
          .param("id", auctionId)
          .update();
      return new BidOutcome(BidResult.CLOSED, null, null, "auction ended");
    }

    BigDecimal minimum = auction.highestBid() != null ? auction.highestBid() : auction.startPrice();
    if (amount.compareTo(minimum) <= 0) {
      jdbc.sql(
              "INSERT INTO bids (auction_id, bidder, amount, accepted) VALUES"
                  + " (:auctionId, :bidder, :amount, false)")
          .param("auctionId", auctionId)
          .param("bidder", bidder)
          .param("amount", amount)
          .update();
      return new BidOutcome(
          BidResult.REJECTED, null, minimum, "bid must be higher than " + minimum);
    }

    int seq = auction.bidCount() + 1;
    jdbc.sql(
            "INSERT INTO bids (auction_id, bidder, amount, accepted, seq) VALUES"
                + " (:auctionId, :bidder, :amount, true, :seq)")
        .param("auctionId", auctionId)
        .param("bidder", bidder)
        .param("amount", amount)
        .param("seq", seq)
        .update();
    jdbc.sql(
            "UPDATE auctions SET highest_bid = :amount, highest_bidder = :bidder,"
                + " bid_count = :seq WHERE id = :id")
        .param("amount", amount)
        .param("bidder", bidder)
        .param("seq", seq)
        .param("id", auctionId)
        .update();
    publisher.publish(auctionId, amount, bidder, seq, auction.endsAt());
    return new BidOutcome(BidResult.ACCEPTED, seq, null, null);
  }
}
