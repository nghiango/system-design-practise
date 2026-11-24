package com.systemdesign.flashsale.seckillapi;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
class SeckillController {

  private final ItemService items;
  private final SaleService sales;
  private final Counter acceptedCounter;
  private final Counter rejectedCounter;

  SeckillController(ItemService items, SaleService sales, MeterRegistry registry) {
    this.items = items;
    this.sales = sales;
    this.acceptedCounter = registry.counter("sales.accepted");
    this.rejectedCounter = registry.counter("sales.rejected");
  }

  @PostMapping("/items")
  ResponseEntity<?> createItem(@RequestBody CreateItemRequest request) {
    if (request == null
        || request.name() == null
        || request.name().isBlank()
        || request.price() == null
        || request.price().signum() < 0
        || request.stock() < 0) {
      return ResponseEntity.badRequest()
          .body(new ErrorResponse("name, non-negative price and stock >= 0 are required"));
    }
    ItemService.Item item = items.create(request.name().trim(), request.price(), request.stock());
    return ResponseEntity.status(HttpStatus.CREATED)
        .body(new ItemResponse(item.id(), item.name(), item.price(), item.stock()));
  }

  @GetMapping("/items")
  List<ItemResponse> listItems() {
    return items.list().stream()
        .map(i -> new ItemResponse(i.id(), i.name(), i.price(), i.stock()))
        .toList();
  }

  @PostMapping("/sales")
  ResponseEntity<?> buy(@RequestBody SaleRequest request) {
    if (request == null || request.orderId() == null || request.orderId().isBlank()) {
      return ResponseEntity.badRequest().body(new ErrorResponse("itemId and orderId are required"));
    }
    SaleService.Result result = sales.buy(request.itemId(), request.orderId());
    return switch (result) {
      case ACCEPTED -> {
        acceptedCounter.increment();
        yield ResponseEntity.status(HttpStatus.ACCEPTED)
            .body(new SaleResponse(request.orderId(), "accepted"));
      }
      case SOLD_OUT -> {
        rejectedCounter.increment();
        yield ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponse("sold out"));
      }
      case ITEM_NOT_FOUND -> {
        rejectedCounter.increment();
        yield ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorResponse("unknown item"));
      }
      case UNAVAILABLE ->
          ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
              .body(new ErrorResponse("sale gate unavailable"));
    };
  }

  record CreateItemRequest(String name, BigDecimal price, int stock) {}

  record ItemResponse(long id, String name, BigDecimal price, int stock) {}

  record SaleRequest(long itemId, String orderId) {}

  record SaleResponse(String orderId, String status) {}

  record ErrorResponse(String error) {}
}
