package com.systemdesign.ticketbooking.bookingworker;

import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/events")
class HistoryController {

  private final JdbcClient jdbc;

  HistoryController(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  /** The full audit trail for an event, oldest first — HELD, CONFIRMED, EXPIRED, ... per seat. */
  @GetMapping("/{id}/history")
  ResponseEntity<List<HistoryEntry>> history(@PathVariable long id) {
    boolean exists =
        jdbc.sql("SELECT 1 FROM events WHERE id = :id")
            .param("id", id)
            .query(Integer.class)
            .optional()
            .isPresent();
    if (!exists) {
      return ResponseEntity.notFound().build();
    }
    return ResponseEntity.ok(
        jdbc.sql(
                "SELECT event_type, seat, owner, at FROM booking_history"
                    + " WHERE event_id = :id ORDER BY id")
            .param("id", id)
            .query(
                (rs, rowNum) ->
                    new HistoryEntry(
                        rs.getString("event_type"),
                        rs.getString("seat"),
                        rs.getString("owner"),
                        rs.getObject("at", OffsetDateTime.class)))
            .list());
  }

  record HistoryEntry(String type, String seat, String owner, OffsetDateTime at) {}
}
