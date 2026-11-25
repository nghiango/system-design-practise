package com.systemdesign.realtimeauction.auctionbroadcast;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

class AuctionSocketHandlerTest {

  private final AuctionSocketHandler handler = new AuctionSocketHandler();

  private WebSocketSession session(String path) {
    WebSocketSession session = mock(WebSocketSession.class);
    Map<String, Object> attributes = new HashMap<>();
    when(session.getUri()).thenReturn(URI.create("ws://localhost" + path));
    when(session.getAttributes()).thenReturn(attributes);
    when(session.getId()).thenReturn("s-" + path);
    return session;
  }

  @Test
  void connectsUnderItsAuctionAndReceivesOnlyItsAuctionsUpdates() throws Exception {
    WebSocketSession on3 = session("/ws/auctions/3");
    WebSocketSession on4 = session("/ws/auctions/4");
    handler.afterConnectionEstablished(on3);
    handler.afterConnectionEstablished(on4);

    assertThat(handler.watchers(3)).isEqualTo(1);
    assertThat(handler.watchers(4)).isEqualTo(1);

    handler.broadcast(3, "{\"auctionId\":3,\"seq\":1}");
    verify(on3).sendMessage(any(TextMessage.class));
    verify(on4, never()).sendMessage(any(TextMessage.class));
  }

  @Test
  void closedSessionIsRemovedAndStopsReceiving() throws Exception {
    WebSocketSession s = session("/ws/auctions/3");
    handler.afterConnectionEstablished(s);
    assertThat(handler.watchers(3)).isEqualTo(1);

    handler.afterConnectionClosed(s, org.springframework.web.socket.CloseStatus.NORMAL);
    assertThat(handler.watchers(3)).isZero();

    handler.broadcast(3, "{}");
    verify(s, never()).sendMessage(any(TextMessage.class));
  }

  @Test
  void failingSessionIsDroppedNotWedgingTheOthers() throws Exception {
    WebSocketSession dead = session("/ws/auctions/3");
    WebSocketSession live = session("/ws/auctions/3");
    handler.afterConnectionEstablished(dead);
    handler.afterConnectionEstablished(live);
    doThrow(new RuntimeException("broken pipe")).when(dead).sendMessage(any(TextMessage.class));

    handler.broadcast(3, "{\"seq\":1}");

    verify(live).sendMessage(any(TextMessage.class));
    assertThat(handler.watchers(3)).isEqualTo(1);
  }

  @Test
  void nonNumericPathIsRejectedAtConnect() throws Exception {
    WebSocketSession bad = session("/ws/auctions/abc");
    handler.afterConnectionEstablished(bad);
    verify(bad).close(any());
    assertThat(handler.watchers(0)).isZero();
  }
}
