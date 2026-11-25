package com.systemdesign.realtimeauction.auctionbroadcast;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

class UpdateConsumerTest {

  private final AuctionSocketHandler handler = mock(AuctionSocketHandler.class);
  private final UpdateConsumer consumer = new UpdateConsumer(handler, new ObjectMapper());

  @Test
  void validUpdateIsFannedOutVerbatimToThatAuction() {
    String message = "{\"auctionId\":7,\"amount\":41.00,\"bidder\":\"bob\",\"seq\":3}";
    consumer.onUpdate(message);
    verify(handler).broadcast(7L, message);
  }

  @Test
  void unparseableUpdateIsDroppedNotWedged() {
    consumer.onUpdate("not json");
    verify(handler, never()).broadcast(anyLong(), anyString());
  }

  @Test
  void updateWithoutAuctionIdIsDropped() {
    consumer.onUpdate("{\"seq\":1}");
    verify(handler, never()).broadcast(anyLong(), anyString());
  }
}
