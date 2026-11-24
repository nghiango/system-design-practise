package com.systemdesign.notification.notifyapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;

class NotificationServiceTest {

  private final JdbcClient jdbc = mock(JdbcClient.class, Mockito.RETURNS_DEEP_STUBS);
  private final NotificationService service = new NotificationService(jdbc);

  @Test
  @SuppressWarnings("unchecked")
  void freshSubmitReturnsCreatedRow() {
    when(jdbc.sql(contains("ON CONFLICT (idempotency_key) DO NOTHING"))
            .param(anyString(), any())
            .param(anyString(), any())
            .param(anyString(), any())
            .param(anyString(), any())
            .query(Long.class)
            .optional())
        .thenReturn(Optional.of(11L));

    NotificationService.Submission s = service.submit("k", "email", "t", "b");

    assertThat(s.created()).isTrue();
    assertThat(s.id()).isEqualTo(11L);
    assertThat(s.status()).isEqualTo("QUEUED");
  }

  @Test
  @SuppressWarnings("unchecked")
  void conflictReturnsExistingRowAndNotCreated() {
    when(jdbc.sql(contains("ON CONFLICT (idempotency_key) DO NOTHING"))
            .param(anyString(), any())
            .param(anyString(), any())
            .param(anyString(), any())
            .param(anyString(), any())
            .query(Long.class)
            .optional())
        .thenReturn(Optional.empty());
    when(jdbc.sql(contains("WHERE idempotency_key = :key"))
            .param(anyString(), any())
            .query(any(RowMapper.class))
            .optional())
        .thenReturn(Optional.of(new NotificationService.Notification(11L, "email", "SENT", 1)));

    NotificationService.Submission s = service.submit("k", "email", "t", "b");

    assertThat(s.created()).isFalse();
    assertThat(s.id()).isEqualTo(11L);
    assertThat(s.status()).isEqualTo("SENT");
  }

  @Test
  void findUsesGuardedIdLookup() {
    service.find(3L);
    verify(jdbc).sql(contains("FROM notifications WHERE id = :id"));
  }
}
