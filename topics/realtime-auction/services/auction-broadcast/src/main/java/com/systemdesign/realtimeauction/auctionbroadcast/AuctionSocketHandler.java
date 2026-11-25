package com.systemdesign.realtimeauction.auctionbroadcast;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * The per-auction session registry behind {@code /ws/auctions/{auctionId}} (decision 002).
 *
 * <p>Sessions are held in a {@code ConcurrentHashMap<auctionId, CopyOnWriteArraySet>}: connect,
 * close and fan-out can race each other — the Kafka consumer thread pushes while HTTP threads add
 * and remove watchers. CopyOnWriteArraySet makes broadcast iteration snapshot-safe, and a session
 * that fails to receive is dropped rather than wedging the send loop for everyone else.
 */
class AuctionSocketHandler extends TextWebSocketHandler {

  private static final Logger log = LoggerFactory.getLogger(AuctionSocketHandler.class);

  private final Map<Long, Set<WebSocketSession>> sessions = new ConcurrentHashMap<>();

  @Override
  public void afterConnectionEstablished(WebSocketSession session) {
    Long auctionId = auctionId(session.getUri());
    if (auctionId == null) {
      closeQuietly(session, CloseStatus.BAD_DATA);
      return;
    }
    session.getAttributes().put("auctionId", auctionId);
    sessions.computeIfAbsent(auctionId, k -> new CopyOnWriteArraySet<>()).add(session);
    log.debug("watcher joined auction {} ({} watching)", auctionId, watchers(auctionId));
  }

  @Override
  public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
    remove(session);
  }

  @Override
  public void handleTransportError(WebSocketSession session, Throwable exception) {
    remove(session);
    closeQuietly(session, CloseStatus.SERVER_ERROR);
  }

  /** Fan-out of one bid update to every watcher of that auction. Failures drop, never wedge. */
  void broadcast(long auctionId, String payload) {
    Set<WebSocketSession> watchers = sessions.get(auctionId);
    if (watchers == null) {
      return;
    }
    TextMessage message = new TextMessage(payload);
    for (WebSocketSession session : watchers) {
      try {
        session.sendMessage(message);
      } catch (Exception e) {
        log.warn("dropping watcher {} of auction {}: {}", session.getId(), auctionId, e.toString());
        remove(session);
        closeQuietly(session, CloseStatus.SERVER_ERROR);
      }
    }
  }

  int watchers(long auctionId) {
    Set<WebSocketSession> watchers = sessions.get(auctionId);
    return watchers == null ? 0 : watchers.size();
  }

  private void remove(WebSocketSession session) {
    Object auctionId = session.getAttributes().get("auctionId");
    if (auctionId instanceof Long id) {
      sessions.computeIfPresent(
          id,
          (k, set) -> {
            set.remove(session);
            return set.isEmpty() ? null : set;
          });
    }
  }

  private static Long auctionId(URI uri) {
    if (uri == null || uri.getPath() == null) {
      return null;
    }
    String path = uri.getPath();
    String last = path.substring(path.lastIndexOf('/') + 1);
    try {
      return Long.valueOf(last);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static void closeQuietly(WebSocketSession session, CloseStatus status) {
    try {
      session.close(status);
    } catch (IOException ignored) {
      // Already gone — nothing to clean.
    }
  }
}
