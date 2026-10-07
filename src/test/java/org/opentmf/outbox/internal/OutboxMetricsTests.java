package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The gauge family: values from the last REFRESH (pending = open-not-parked + parked), relay-lag
 * computed at read time from the oldest open row's instant, NaN before the first refresh and
 * after a failed one - and a scrape never touches the database.
 */
class OutboxMetricsTests {

  private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final OutboxMetrics metrics =
      new OutboxMetrics(registry, repository, Duration.ofSeconds(15));

  private double gauge(String name) {
    return registry.get(name).gauge().value();
  }

  @Test
  void beforeTheFirstRefresh_theGaugesReadNaN_neverAMisleadingZero() {
    assertThat(gauge(OutboxMetrics.PENDING)).isNaN();
    assertThat(gauge(OutboxMetrics.PARKED)).isNaN();
    assertThat(gauge(OutboxMetrics.IN_FLIGHT)).isNaN();
    assertThat(gauge(OutboxMetrics.RELAY_LAG)).isNaN();
    assertThat(gauge(OutboxMetrics.METRICS_AGE)).isNaN(); // never refreshed
  }

  @Test
  void theSnapshotsAge_restartsOnEverySuccess_andKeepsGrowingThroughFailures() throws Exception {
    metrics.refresh();
    assertThat(gauge(OutboxMetrics.METRICS_AGE)).isBetween(0d, 1d);

    when(repository.countParked()).thenThrow(new IllegalStateException("database gone"));
    assertThat(new CountDownLatch(1).await(1100, TimeUnit.MILLISECONDS)).isFalse(); // time passes
    metrics.refresh(); // fails: the values go NaN, the age does NOT restart

    assertThat(gauge(OutboxMetrics.PARKED)).isNaN();
    // bounded BOTH ways: ~1.1 s, in SECONDS (the millis-to-seconds division is load-bearing)
    assertThat(gauge(OutboxMetrics.METRICS_AGE)).isBetween(1d, 5d);
  }

  @Test
  void aRefresh_setsTheGauges_pendingIncludesParked() {
    when(repository.countOpenNotParked()).thenReturn(4L);
    when(repository.countParked()).thenReturn(1L);
    when(repository.countInFlight(any(OffsetDateTime.class))).thenReturn(3L);
    when(repository.findOldestOpenSince()).thenReturn(Optional.empty());

    metrics.refresh();

    assertThat(gauge(OutboxMetrics.PENDING)).isEqualTo(5d);
    assertThat(gauge(OutboxMetrics.PARKED)).isEqualTo(1d);
    assertThat(gauge(OutboxMetrics.IN_FLIGHT)).isEqualTo(3d);
    assertThat(gauge(OutboxMetrics.RELAY_LAG)).isZero(); // nothing open
  }

  @Test
  void relayLag_isTheOldestOpenRowsAge_asOfReadTime_zeroWhileItIsHeld() {
    when(repository.findOldestOpenSince())
        .thenReturn(Optional.of(Instant.now().minusSeconds(90)));
    metrics.refresh();
    // bounded BOTH ways: ~90s, in SECONDS (the millis-to-seconds division is load-bearing)
    assertThat(metrics.relayLagSeconds()).isBetween(85d, 95d);

    when(repository.findOldestOpenSince())
        .thenReturn(Optional.of(Instant.now().plusSeconds(3600))); // only held rows open
    metrics.refresh();
    assertThat(metrics.relayLagSeconds()).isZero();
  }

  @Test
  void aFailedRefresh_readsNaN_untilTheNextOneSucceeds() {
    when(repository.countParked()).thenReturn(1L);
    metrics.refresh();
    assertThat(gauge(OutboxMetrics.PARKED)).isEqualTo(1d);

    when(repository.countParked()).thenThrow(new IllegalStateException("database gone"));
    metrics.refresh();

    assertThat(gauge(OutboxMetrics.PARKED)).isNaN();
    assertThat(gauge(OutboxMetrics.RELAY_LAG)).isNaN();
  }

  @Test
  void theRefresher_runsOnStart_onADaemonThread_andStops() {
    when(repository.countParked()).thenReturn(2L);

    metrics.start();
    try {
      await().atMost(Duration.ofSeconds(5)).until(() -> gauge(OutboxMetrics.PARKED) == 2d);
      assertThat(
              Thread.getAllStackTraces().keySet().stream()
                  .filter(t -> "opentmf-outbox-metrics".equals(t.getName()))
                  .findFirst())
          .hasValueSatisfying(t -> assertThat(t.isDaemon()).isTrue());
    } finally {
      metrics.stop();
    }
    new OutboxMetrics(new SimpleMeterRegistry(), repository, Duration.ofSeconds(1)).stop();
  }

  @Test
  void recordDropped_countsPerDestination_neverAsRelayed() {
    metrics.recordDropped("hub");

    assertThat(
            registry
                .get(OutboxMetrics.DROPPED)
                .tag(OutboxMetrics.TAG_DESTINATION, "hub")
                .counter()
                .count())
        .isEqualTo(1d);
    assertThat(registry.find(OutboxMetrics.RELAYED).counters()).isEmpty();
  }

  /**
   * F-2 (load test 2026-10-06): a scrape must never hold a thread on the database - reading
   * every gauge reads the last refreshed values, never the repository.
   */
  @Test
  void aScrape_neverTouchesTheDatabase() {
    registry.getMeters().forEach(meter -> meter.measure().forEach(m -> m.getValue()));

    verifyNoInteractions(repository);
  }
}
