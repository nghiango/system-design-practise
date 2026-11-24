package com.systemdesign.notification.notifyapi;

import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

/**
 * Idempotent submit path. {@code notifications.idempotency_key} is UNIQUE and the insert uses
 * {@code ON CONFLICT DO NOTHING}: a retry of the same submit returns the existing row instead of
 * queueing a second send, which is what makes at-least-once client retries safe end to end.
 */
@Service
class NotificationService {

  private final JdbcClient jdbc;

  NotificationService(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  /**
   * Inserts the notification or returns the row already stored under {@code idempotencyKey}. The
   * {@code created} flag tells the caller whether it must publish the send event.
   */
  Submission submit(String idempotencyKey, String channel, String target, String body) {
    Optional<Long> inserted =
        jdbc.sql(
                "INSERT INTO notifications (idempotency_key, channel, target, body) VALUES"
                    + " (:key, :channel, :target, :body) ON CONFLICT (idempotency_key) DO NOTHING"
                    + " RETURNING id")
            .param("key", idempotencyKey)
            .param("channel", channel)
            .param("target", target)
            .param("body", body)
            .query(Long.class)
            .optional();
    if (inserted.isPresent()) {
      return new Submission(inserted.get(), "QUEUED", true);
    }
    Notification existing = findByKey(idempotencyKey).orElseThrow();
    return new Submission(existing.id(), existing.status(), false);
  }

  Optional<Notification> find(long id) {
    return jdbc.sql("SELECT id, channel, status, attempts FROM notifications WHERE id = :id")
        .param("id", id)
        .query(
            (rs, rowNum) ->
                new Notification(
                    rs.getLong("id"),
                    rs.getString("channel"),
                    rs.getString("status"),
                    rs.getInt("attempts")))
        .optional();
  }

  private Optional<Notification> findByKey(String idempotencyKey) {
    return jdbc.sql(
            "SELECT id, channel, status, attempts FROM notifications WHERE idempotency_key = :key")
        .param("key", idempotencyKey)
        .query(
            (rs, rowNum) ->
                new Notification(
                    rs.getLong("id"),
                    rs.getString("channel"),
                    rs.getString("status"),
                    rs.getInt("attempts")))
        .optional();
  }

  record Submission(long id, String status, boolean created) {}

  record Notification(long id, String channel, String status, int attempts) {}
}
