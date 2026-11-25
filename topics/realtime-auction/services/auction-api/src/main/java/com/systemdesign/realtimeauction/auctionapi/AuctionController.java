package com.systemdesign.realtimeauction.auctionapi;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/auctions")
class AuctionController {

  private final AuctionService service;
  private final Counter acceptedCounter;
  private final Counter rejectedCounter;
  private final Counter closedCounter;

  AuctionController(AuctionService service, MeterRegistry registry) {
    this.service = service;
    this.acceptedCounter = registry.counter("bids.accepted");
    this.rejectedCounter = registry.counter("bids.rejected");
    this.closedCounter = registry.counter("bids.closed");
  }

  @PostMapping
  ResponseEntity<?> createAuction(@RequestBody CreateAuctionRequest request) {
    if (request == null
        || request.title() == null
        || request.title().isBlank()
        || request.startPrice() == null
        || request.startPrice().signum() < 0
        || request.endsInSec() <= 0) {
      return ResponseEntity.badRequest()
          .body(new ErrorResponse("title, a non-negative startPrice and endsInSec are required"));
    }
    long id =
        service.createAuction(request.title().trim(), request.startPrice(), request.endsInSec());
    return ResponseEntity.status(HttpStatus.CREATED).body(new AuctionResponse(id));
  }

  @GetMapping("/{id}")
  ResponseEntity<AuctionState> auction(@PathVariable long id) {
    return service
        .auction(id)
        .map(
            a ->
                new AuctionState(
                    a.highestBid(), a.highestBidder(), a.bidCount(), a.status(), a.endsAt()))
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  @GetMapping("/{id}/bids")
  ResponseEntity<List<BidEntry>> bids(@PathVariable long id) {
    if (!service.auctionExists(id)) {
      return ResponseEntity.notFound().build();
    }
    return ResponseEntity.ok(
        service.acceptedBids(id).stream()
            .map(b -> new BidEntry(b.bidder(), b.amount(), b.seq(), b.at()))
            .toList());
  }

  @PostMapping("/{id}/bids")
  ResponseEntity<?> bid(@PathVariable long id, @RequestBody BidRequest request) {
    if (request == null
        || request.bidder() == null
        || request.bidder().isBlank()
        || request.amount() == null
        || request.amount().signum() <= 0) {
      return ResponseEntity.badRequest()
          .body(new ErrorResponse("bidder and a positive amount are required"));
    }
    AuctionService.BidOutcome outcome =
        service.placeBid(id, request.bidder().trim(), request.amount());
    return switch (outcome.result()) {
      case ACCEPTED -> {
        acceptedCounter.increment();
        yield ResponseEntity.ok(new BidResponse(outcome.seq()));
      }
      case REJECTED -> {
        rejectedCounter.increment();
        yield ResponseEntity.status(HttpStatus.CONFLICT)
            .body(new RejectResponse(outcome.reason(), outcome.minimum()));
      }
      case CLOSED -> {
        closedCounter.increment();
        yield ResponseEntity.status(HttpStatus.GONE).body(new ErrorResponse(outcome.reason()));
      }
      case NOT_FOUND ->
          ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorResponse(outcome.reason()));
    };
  }

  record CreateAuctionRequest(String title, BigDecimal startPrice, long endsInSec) {}

  record AuctionResponse(long id) {}

  record BidRequest(String bidder, BigDecimal amount) {}

  record BidResponse(int seq) {}

  record RejectResponse(String reason, BigDecimal minimum) {}

  record AuctionState(
      BigDecimal highestBid,
      String highestBidder,
      int bidCount,
      String status,
      OffsetDateTime endsAt) {}

  record BidEntry(String bidder, BigDecimal amount, int seq, OffsetDateTime at) {}

  record ErrorResponse(String error) {}
}
