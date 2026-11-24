package com.systemdesign.notification.senderworker;

import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Read side for verification — same response shape as notify-api's GET. */
@RestController
@RequestMapping("/api/notifications")
class NotificationController {

  private final JdbcClient jdbc;

  NotificationController(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  @GetMapping("/{id}")
  ResponseEntity<NotificationResponse> get(@PathVariable long id) {
    return jdbc.sql("SELECT id, channel, status, attempts FROM notifications WHERE id = :id")
        .param("id", id)
        .query(
            (rs, rowNum) ->
                new NotificationResponse(
                    rs.getLong("id"),
                    rs.getString("channel"),
                    rs.getString("status"),
                    rs.getInt("attempts")))
        .optional()
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  record NotificationResponse(long id, String channel, String status, int attempts) {}
}
