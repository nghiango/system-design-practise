package com.systemdesign.flashsale.orderworker;

import java.time.OffsetDateTime;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
class OrderController {

  private final JdbcClient jdbc;

  OrderController(JdbcClient jdbc) {
    this.jdbc = jdbc;
  }

  @GetMapping("/{orderId}")
  ResponseEntity<OrderResponse> get(@PathVariable String orderId) {
    return jdbc.sql("SELECT order_id, item_id, status, created_at FROM orders WHERE order_id = :id")
        .param("id", orderId)
        .query(
            (rs, rowNum) ->
                new OrderResponse(
                    rs.getString("order_id"),
                    rs.getLong("item_id"),
                    rs.getString("status"),
                    rs.getObject("created_at", OffsetDateTime.class)))
        .optional()
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  record OrderResponse(String orderId, long itemId, String status, OffsetDateTime createdAt) {}
}
