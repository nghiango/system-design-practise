package com.systemdesign.realtimeauction.auctionapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;

class AuctionServiceTest {

  private JdbcClient jdbc;
  private BidEventPublisher publisher;
  private AuctionService service;

  /** sql fragment → every completed param-map passed to .update() on statements matching it. */
  private final Map<String, List<Map<String, Object>>> executedUpdates = new LinkedHashMap<>();

  // Test-controlled stub outcomes (set per test before calling the service).
  private AuctionService.LockedAuction lockedAuction; // null → FOR UPDATE finds no row → 404
  private long insertReturnId;

  @BeforeEach
  void setUp() {
    jdbc = mock(JdbcClient.class, Mockito.RETURNS_DEEP_STUBS);
    publisher = mock(BidEventPublisher.class);
    service = new AuctionService(jdbc, publisher);
    insertReturnId = 1L;
    lockedAuction =
        new AuctionService.LockedAuction(
            "OPEN", new BigDecimal("10.00"), null, null, OffsetDateTime.now().plusHours(1), 0);
    when(jdbc.sql(anyString())).thenAnswer(inv -> recordingSpec(inv.getArgument(0)));
  }

  private JdbcClient.StatementSpec recordingSpec(String sql) {
    JdbcClient.StatementSpec spec =
        mock(JdbcClient.StatementSpec.class, Mockito.RETURNS_DEEP_STUBS);
    Map<String, Object> current = new LinkedHashMap<>();
    when(spec.param(anyString(), any()))
        .thenAnswer(
            p -> {
              current.put(p.getArgument(0), p.getArgument(1));
              return spec;
            });
    when(spec.param(anyString(), any(), anyInt()))
        .thenAnswer(
            p -> {
              current.put(p.getArgument(0), p.getArgument(1));
              return spec;
            });
    when(spec.update())
        .thenAnswer(
            u -> {
              executedUpdates.computeIfAbsent(sql, k -> new ArrayList<>()).add(Map.copyOf(current));
              current.clear();
              return 1;
            });
    @SuppressWarnings("unchecked")
    JdbcClient.MappedQuerySpec<Long> longQuery = mock(JdbcClient.MappedQuerySpec.class);
    when(longQuery.single()).thenAnswer(o -> insertReturnId);
    when(longQuery.optional()).thenAnswer(o -> Optional.of(insertReturnId));
    when(spec.query(Long.class)).thenReturn(longQuery);
    @SuppressWarnings("unchecked")
    JdbcClient.MappedQuerySpec<Object> rowQuery = mock(JdbcClient.MappedQuerySpec.class);
    when(rowQuery.optional()).thenAnswer(o -> Optional.ofNullable(lockedAuction));
    when(spec.query(any(RowMapper.class))).thenReturn((JdbcClient.MappedQuerySpec) rowQuery);
    return spec;
  }

  private List<Map<String, Object>> updatesMatching(String fragment) {
    return executedUpdates.entrySet().stream()
        .filter(e -> e.getKey().contains(fragment))
        .flatMap(e -> e.getValue().stream())
        .toList();
  }

  @Test
  void missingAuctionIsNotFoundAndWritesNothing() {
    lockedAuction = null;

    AuctionService.BidOutcome outcome = service.placeBid(9L, "alice", new BigDecimal("50"));

    assertThat(outcome.result()).isEqualTo(AuctionService.BidResult.NOT_FOUND);
    verify(jdbc, never()).sql(contains("INSERT INTO bids"));
    verify(jdbc, never()).sql(contains("UPDATE auctions"));
    verify(publisher, never()).publish(anyLong(), any(), any(), anyInt(), any());
  }

  @Test
  void closedAuctionIs410AndWritesNothing() {
    lockedAuction =
        new AuctionService.LockedAuction(
            "CLOSED",
            new BigDecimal("10.00"),
            new BigDecimal("40.00"),
            "alice",
            OffsetDateTime.now().plusHours(1),
            1);

    AuctionService.BidOutcome outcome = service.placeBid(1L, "bob", new BigDecimal("100"));

    assertThat(outcome.result()).isEqualTo(AuctionService.BidResult.CLOSED);
    verify(jdbc, never()).sql(contains("INSERT INTO bids"));
    verify(jdbc, never()).sql(contains("UPDATE auctions"));
    verify(publisher, never()).publish(anyLong(), any(), any(), anyInt(), any());
  }

  @Test
  void expiredOpenAuctionFlipsToClosedAndRejectsTheBid() {
    lockedAuction =
        new AuctionService.LockedAuction(
            "OPEN",
            new BigDecimal("10.00"),
            new BigDecimal("40.00"),
            "alice",
            OffsetDateTime.now().minusSeconds(5),
            1);

    AuctionService.BidOutcome outcome = service.placeBid(1L, "bob", new BigDecimal("100"));

    assertThat(outcome.result()).isEqualTo(AuctionService.BidResult.CLOSED);
    List<Map<String, Object>> closes = updatesMatching("SET status = 'CLOSED'");
    assertThat(closes).hasSize(1);
    assertThat(closes.get(0)).containsEntry("id", 1L);
    // The expired bid is never recorded — the auction was already over.
    verify(jdbc, never()).sql(contains("INSERT INTO bids"));
    verify(publisher, never()).publish(anyLong(), any(), any(), anyInt(), any());
  }

  @Test
  void bidAtOrBelowTheCurrentHighestIsRecordedRejected() {
    lockedAuction =
        new AuctionService.LockedAuction(
            "OPEN",
            new BigDecimal("10.00"),
            new BigDecimal("40.00"),
            "alice",
            OffsetDateTime.now().plusHours(1),
            2);

    AuctionService.BidOutcome outcome = service.placeBid(1L, "bob", new BigDecimal("40.00"));

    assertThat(outcome.result()).isEqualTo(AuctionService.BidResult.REJECTED);
    assertThat(outcome.minimum()).isEqualByComparingTo("40.00");
    List<Map<String, Object>> rows = updatesMatching("INSERT INTO bids");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0))
        .containsEntry("auctionId", 1L)
        .containsEntry("bidder", "bob")
        .containsEntry("amount", new BigDecimal("40.00"));
    // No seq, no highest update, no event — a losing bid changes nothing but the audit trail.
    verify(jdbc, never()).sql(contains("highest_bid ="));
    verify(publisher, never()).publish(anyLong(), any(), any(), anyInt(), any());
  }

  @Test
  void firstBidMustBeatTheStartPrice() {
    lockedAuction =
        new AuctionService.LockedAuction(
            "OPEN", new BigDecimal("10.00"), null, null, OffsetDateTime.now().plusHours(1), 0);

    AuctionService.BidOutcome outcome = service.placeBid(1L, "alice", new BigDecimal("10.00"));

    assertThat(outcome.result()).isEqualTo(AuctionService.BidResult.REJECTED);
    assertThat(outcome.minimum()).isEqualByComparingTo("10.00");
  }

  @Test
  void acceptedBidTakesSeqOneUpdatesHighestAndPublishesKeyedByAuctionId() {
    lockedAuction =
        new AuctionService.LockedAuction(
            "OPEN",
            new BigDecimal("10.00"),
            new BigDecimal("40.00"),
            "alice",
            OffsetDateTime.now().plusHours(1),
            2);

    AuctionService.BidOutcome outcome = service.placeBid(7L, "bob", new BigDecimal("41.00"));

    assertThat(outcome.result()).isEqualTo(AuctionService.BidResult.ACCEPTED);
    assertThat(outcome.seq()).isEqualTo(3);

    List<Map<String, Object>> rows = updatesMatching("INSERT INTO bids");
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0))
        .containsEntry("seq", 3)
        .containsEntry("amount", new BigDecimal("41.00"));

    List<Map<String, Object>> highest = updatesMatching("highest_bid = :amount");
    assertThat(highest).hasSize(1);
    assertThat(highest.get(0))
        .containsEntry("amount", new BigDecimal("41.00"))
        .containsEntry("bidder", "bob")
        .containsEntry("seq", 3)
        .containsEntry("id", 7L);

    // The Kafka key IS the ordering guarantee: key = auctionId → one partition → seq order.
    verify(publisher)
        .publish(eq(7L), eq(new BigDecimal("41.00")), eq("bob"), eq(3), eq(lockedAuction.endsAt()));
  }
}
