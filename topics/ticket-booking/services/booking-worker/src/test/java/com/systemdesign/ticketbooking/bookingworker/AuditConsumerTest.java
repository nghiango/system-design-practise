package com.systemdesign.ticketbooking.bookingworker;

import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.ObjectMapper;

class AuditConsumerTest {

  private JdbcClient jdbc;
  private AuditConsumer consumer;

  @BeforeEach
  void setUp() {
    jdbc = mock(JdbcClient.class, Mockito.RETURNS_DEEP_STUBS);
    consumer = new AuditConsumer(jdbc, new ObjectMapper());
  }

  @Test
  void validEventAppendsToBookingHistory() {
    consumer.onEvent("{\"type\":\"HELD\",\"eventId\":3,\"seat\":\"C4\",\"owner\":\"alice\"}");

    verify(jdbc).sql(contains("INSERT INTO booking_history"));
  }

  @Test
  void malformedEventIsDroppedNotWedged() {
    consumer.onEvent("not json");

    verify(jdbc, never()).sql(contains("INSERT INTO booking_history"));
  }
}
