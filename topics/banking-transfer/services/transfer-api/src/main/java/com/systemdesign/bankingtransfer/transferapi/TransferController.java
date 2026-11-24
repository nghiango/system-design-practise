package com.systemdesign.bankingtransfer.transferapi;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
class TransferController {

  private final TransferService service;
  private final Counter completedCounter;
  private final Counter rejectedCounter;
  private final Counter replayedCounter;

  TransferController(TransferService service, MeterRegistry registry) {
    this.service = service;
    this.completedCounter = registry.counter("transfers.completed");
    this.rejectedCounter = registry.counter("transfers.rejected");
    this.replayedCounter = registry.counter("transfers.replayed");
  }

  // ---- accounts ----------------------------------------------------------------------------

  @PostMapping("/accounts")
  ResponseEntity<?> createAccount(@RequestBody CreateAccountRequest request) {
    if (request == null
        || request.owner() == null
        || request.owner().isBlank()
        || request.initial() == null
        || request.initial().signum() < 0) {
      return ResponseEntity.badRequest()
          .body(new ErrorResponse("owner and a non-negative initial balance are required"));
    }
    try {
      long id = service.createAccount(request.owner().trim(), request.initial());
      return ResponseEntity.status(HttpStatus.CREATED).body(new AccountResponse(id));
    } catch (DuplicateKeyException e) {
      return ResponseEntity.status(HttpStatus.CONFLICT)
          .body(new ErrorResponse("owner already exists"));
    }
  }

  @GetMapping("/accounts/{id}")
  ResponseEntity<AccountBalanceResponse> account(@PathVariable long id) {
    return service
        .account(id)
        .map(a -> new AccountBalanceResponse(a.id(), a.owner(), a.balance()))
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  @GetMapping("/accounts/{id}/statement")
  ResponseEntity<List<StatementEntry>> statement(@PathVariable long id) {
    if (!service.accountExists(id)) {
      return ResponseEntity.notFound().build();
    }
    return ResponseEntity.ok(
        service.statement(id).stream()
            .map(
                e ->
                    new StatementEntry(
                        e.transferId(), e.entry(), e.amount(), e.balanceAfter(), e.at()))
            .toList());
  }

  // ---- transfers ----------------------------------------------------------------------------

  @PostMapping("/transfers")
  ResponseEntity<?> transfer(@RequestBody TransferRequest request) {
    if (request == null
        || request.idempotencyKey() == null
        || request.idempotencyKey().isBlank()
        || request.amount() == null
        || request.amount().signum() <= 0) {
      return ResponseEntity.badRequest()
          .body(new ErrorResponse("idempotencyKey and a positive amount are required"));
    }
    if (request.from() == request.to()) {
      return ResponseEntity.badRequest()
          .body(new ErrorResponse("from and to must be different accounts"));
    }
    TransferService.TransferOutcome outcome =
        service.transfer(
            request.idempotencyKey().trim(), request.from(), request.to(), request.amount());
    return switch (outcome.result()) {
      case COMPLETED -> {
        completedCounter.increment();
        yield ResponseEntity.status(HttpStatus.CREATED).body(toResponse(outcome.transfer(), false));
      }
      case REJECTED -> {
        rejectedCounter.increment();
        yield ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
            .body(toResponse(outcome.transfer(), false));
      }
      case REPLAYED -> {
        replayedCounter.increment();
        // The stored result is returned verbatim — same id, same terminal status, no second write.
        yield ResponseEntity.ok(toResponse(outcome.transfer(), true));
      }
    };
  }

  @GetMapping("/transfers/{id}")
  ResponseEntity<TransferResponse> transferById(@PathVariable long id) {
    return service
        .transfer(id)
        .map(t -> toResponse(t, false))
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  @GetMapping("/transfers/key/{key}")
  ResponseEntity<TransferResponse> transferByKey(@PathVariable String key) {
    return service
        .transferByKey(key)
        .map(t -> toResponse(t, false))
        .map(ResponseEntity::ok)
        .orElseGet(() -> ResponseEntity.notFound().build());
  }

  private static TransferResponse toResponse(TransferService.TransferView t, boolean replayed) {
    return new TransferResponse(t.id(), t.status(), t.from(), t.to(), t.amount(), replayed);
  }

  @ExceptionHandler(TransferService.AccountNotFoundException.class)
  ResponseEntity<ErrorResponse> accountNotFound(TransferService.AccountNotFoundException e) {
    return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorResponse(e.getMessage()));
  }

  record CreateAccountRequest(String owner, BigDecimal initial) {}

  record AccountResponse(long id) {}

  record AccountBalanceResponse(long id, String owner, BigDecimal balance) {}

  record TransferRequest(String idempotencyKey, long from, long to, BigDecimal amount) {}

  record TransferResponse(
      long id, String status, long from, long to, BigDecimal amount, boolean replayed) {}

  record StatementEntry(
      long transferId,
      String entry,
      BigDecimal amount,
      BigDecimal balanceAfter,
      OffsetDateTime at) {}

  record ErrorResponse(String error) {}
}
