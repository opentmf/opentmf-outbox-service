package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.opentmf.outbox.internal.OutboxEventRepository.LaneCounts;
import org.opentmf.outbox.internal.OutboxEventRepository.LaneSince;

/**
 * The gauge family: values from the last REFRESH (pending = open-not-parked + parked), relay-lag
 * computed at read time from the oldest open row's instant, NaN before the first refresh and
 * after a failed one - and a scrape never touches the database. Since 1.4.0 every row gauge is
 * two series by {@code lane}; {@code metrics-age} stays one.
 */
class OutboxMetricsTests {

  private static final String ORDERED = "ordered";
  private static final String CONCURRENT = "concurrent";

  private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final OutboxMetrics metrics =
      new OutboxMetrics(registry, repository, Duration.ofSeconds(15));

  private static LaneCounts counts(long ordered, long concurrent) {
    return new LaneCounts() {
      @Override
      public long getOrdered() {
        return ordered;
      }

      @Override
      public long getConcurrent() {
        return concurrent;
      }
    };
  }

  private static LaneSince since(Instant ordered, Instant concurrent) {
    return new LaneSince() {
      @Override
      public Instant getOrdered() {
        return ordered;
      }

      @Override
      public Instant getConcurrent() {
        return concurrent;
      }
    };
  }

  /** An empty outbox, unless a test says otherwise. */
  @BeforeEach
  void anEmptyOutbox() {
    when(repository.countOpenNotParked()).thenReturn(counts(0, 0));
    when(repository.countParked()).thenReturn(counts(0, 0));
    when(repository.countInFlight(any(OffsetDateTime.class))).thenReturn(counts(0, 0));
    when(repository.findOldestOpenSince()).thenReturn(since(null, null));
  }

  private double gauge(String name, String lane) {
    return registry.get(name).tag(OutboxMetrics.TAG_LANE, lane).gauge().value();
  }

  private double gauge(String name) {
    return registry.get(name).gauge().value();
  }

  @Test
  void everyRowGauge_isOneSeriesPerLane_andMetricsAgeIsOne() {
    for (String name :
        List.of(
            OutboxMetrics.PENDING,
            OutboxMetrics.PARKED,
            OutboxMetrics.IN_FLIGHT,
            OutboxMetrics.RELAY_LAG)) {
      assertThat(registry.find(name).gauges())
          .as(name)
          .extracting(g -> g.getId().getTag(OutboxMetrics.TAG_LANE))
          .containsExactlyInAnyOrder(ORDERED, CONCURRENT);
    }
    assertThat(registry.find(OutboxMetrics.METRICS_AGE).gauges())
        .singleElement()
        .satisfies(g -> assertThat(g.getId().getTags()).isEmpty());
  }

  @Test
  void beforeTheFirstRefresh_theGaugesReadNaN_neverAMisleadingZero() {
    for (String lane : List.of(ORDERED, CONCURRENT)) {
      assertThat(gauge(OutboxMetrics.PENDING, lane)).isNaN();
      assertThat(gauge(OutboxMetrics.PARKED, lane)).isNaN();
      assertThat(gauge(OutboxMetrics.IN_FLIGHT, lane)).isNaN();
      assertThat(gauge(OutboxMetrics.RELAY_LAG, lane)).isNaN();
    }
    assertThat(gauge(OutboxMetrics.METRICS_AGE)).isNaN(); // never refreshed
  }

  @Test
  void theSnapshotsAge_restartsOnEverySuccess_andKeepsGrowingThroughFailures() throws Exception {
    metrics.refresh();
    assertThat(gauge(OutboxMetrics.METRICS_AGE)).isBetween(0d, 1d);

    when(repository.countParked()).thenThrow(new IllegalStateException("database gone"));
    assertThat(new CountDownLatch(1).await(1100, TimeUnit.MILLISECONDS)).isFalse(); // time passes
    metrics.refresh(); // fails: the values go NaN, the age does NOT restart

    assertThat(gauge(OutboxMetrics.PARKED, ORDERED)).isNaN();
    // bounded BOTH ways: ~1.1 s, in SECONDS (the millis-to-seconds division is load-bearing)
    assertThat(gauge(OutboxMetrics.METRICS_AGE)).isBetween(1d, 5d);
  }

  @Test
  void aRefresh_setsEachLanesGauges_pendingIncludesParked() {
    when(repository.countOpenNotParked()).thenReturn(counts(4, 40));
    when(repository.countParked()).thenReturn(counts(1, 10));
    when(repository.countInFlight(any(OffsetDateTime.class))).thenReturn(counts(1, 8));

    metrics.refresh();

    assertThat(gauge(OutboxMetrics.PENDING, ORDERED)).isEqualTo(5d);
    assertThat(gauge(OutboxMetrics.PENDING, CONCURRENT)).isEqualTo(50d);
    assertThat(gauge(OutboxMetrics.PARKED, ORDERED)).isEqualTo(1d);
    assertThat(gauge(OutboxMetrics.PARKED, CONCURRENT)).isEqualTo(10d);
    assertThat(gauge(OutboxMetrics.IN_FLIGHT, ORDERED)).isEqualTo(1d);
    assertThat(gauge(OutboxMetrics.IN_FLIGHT, CONCURRENT)).isEqualTo(8d);
    assertThat(gauge(OutboxMetrics.RELAY_LAG, ORDERED)).isZero(); // nothing open in either
    assertThat(gauge(OutboxMetrics.RELAY_LAG, CONCURRENT)).isZero();
  }

  @Test
  void relayLag_isEachLanesOldestOpenRowsAge_asOfReadTime_zeroWhileItIsHeld() {
    when(repository.findOldestOpenSince())
        .thenReturn(since(Instant.now().minusSeconds(90), Instant.now().minusSeconds(600)));
    metrics.refresh();
    // bounded BOTH ways: in SECONDS (the millis-to-seconds division is load-bearing), and the
    // slow CONCURRENT subscriber's lag does not leak into the ORDERED series
    assertThat(metrics.relayLagSeconds(Lane.ORDERED)).isBetween(85d, 95d);
    assertThat(metrics.relayLagSeconds(Lane.CONCURRENT)).isBetween(595d, 605d);

    when(repository.findOldestOpenSince())
        .thenReturn(since(Instant.now().plusSeconds(3600), null)); // only held rows open
    metrics.refresh();
    assertThat(metrics.relayLagSeconds(Lane.ORDERED)).isZero();
    assertThat(metrics.relayLagSeconds(Lane.CONCURRENT)).isZero();
  }

  @Test
  void aFailedRefresh_readsNaN_untilTheNextOneSucceeds() {
    when(repository.countParked()).thenReturn(counts(1, 0));
    metrics.refresh();
    assertThat(gauge(OutboxMetrics.PARKED, ORDERED)).isEqualTo(1d);

    when(repository.countParked()).thenThrow(new IllegalStateException("database gone"));
    metrics.refresh();

    for (String lane : List.of(ORDERED, CONCURRENT)) {
      assertThat(gauge(OutboxMetrics.PARKED, lane)).isNaN();
      assertThat(gauge(OutboxMetrics.RELAY_LAG, lane)).isNaN();
    }
  }

  @Test
  void theRefresher_runsOnStart_onADaemonThread_andStops() {
    when(repository.countParked()).thenReturn(counts(0, 2));

    metrics.start();
    try {
      await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> gauge(OutboxMetrics.PARKED, CONCURRENT) == 2d);
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
    clearInvocations(repository);
    registry.getMeters().forEach(meter -> meter.measure().forEach(m -> m.getValue()));

    verifyNoInteractions(repository);
  }
}
