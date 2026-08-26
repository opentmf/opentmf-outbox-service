package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.OffsetDateTime;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxProperties;

/** The gauge family: pending/parked wired to the repository, lag from the oldest pending. */
class OutboxMetricsTests {

  private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final OutboxMetrics metrics =
      new OutboxMetrics(registry, repository, new OutboxProperties());

  @Test
  void gauges_readTheRepository() {
    when(repository.countByRelayedOnIsNull()).thenReturn(4L);
    when(repository.countByRelayedOnIsNullAndAttemptsGreaterThanEqual(10)).thenReturn(1L);

    assertThat(registry.get(OutboxMetrics.PENDING).gauge().value()).isEqualTo(4d);
    assertThat(registry.get(OutboxMetrics.PARKED).gauge().value()).isEqualTo(1d);
  }

  @Test
  void relayLag_isTheOldestPendingAge_zeroWhenNonePending() {
    when(repository.findOldestPendingCreatedOn())
        .thenReturn(Optional.of(OffsetDateTime.now().minusSeconds(90)));
    // bounded BOTH ways: ~90s, in SECONDS (the millis-to-seconds division is load-bearing)
    assertThat(metrics.relayLagSeconds()).isBetween(85d, 95d);

    when(repository.findOldestPendingCreatedOn()).thenReturn(Optional.empty());
    assertThat(metrics.relayLagSeconds()).isZero();
  }

  @Test
  void recordRelayed_countsPerDestination_andBooksAttempts() {
    metrics.recordRelayed("comm.delivery.v1", 3);
    metrics.recordRelayed("comm.delivery.v1", 1);

    assertThat(
            registry
                .get(OutboxMetrics.RELAYED)
                .tag(OutboxMetrics.TAG_DESTINATION, "comm.delivery.v1")
                .counter()
                .count())
        .isEqualTo(2d);
    assertThat(registry.get(OutboxMetrics.ATTEMPTS).summary().totalAmount()).isEqualTo(4d);
  }
}
