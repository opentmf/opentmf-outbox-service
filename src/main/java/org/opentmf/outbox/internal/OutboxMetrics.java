package org.opentmf.outbox.internal;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * The outbox meter family under the LIBRARY-STABLE names ({@code opentmf.outbox.*}
 * - one name across every consumer; the emitting service is distinguished by the registry's
 * common tags / scrape identity, never by a per-service metric prefix):
 *
 * <ul>
 *   <li>{@code opentmf.outbox.pending} - gauge, rows not yet relayed nor cancelled (held, parked
 *       and in-flight rows included)
 *   <li>{@code opentmf.outbox.in-flight} - gauge, pending rows under a live lease: claimed by a
 *       relay (any pod) whose send has not been booked yet (1.3.0)
 *   <li>{@code opentmf.outbox.parked} - gauge, alert when above 0
 *   <li>{@code opentmf.outbox.relay-lag} - gauge, how long the oldest RELEASED pending row has
 *       been deliverable (seconds) - a held row is not lagging until its hold passes
 *   <li>{@code opentmf.outbox.metrics-age} - gauge, seconds since the last SUCCESSFUL refresh of
 *       the gauges above (NaN before the first) - alert when it exceeds a few refresh periods
 *   <li>{@code opentmf.outbox.relayed} - counter by {@code destination} (closed tag set)
 *   <li>{@code opentmf.outbox.dropped} - counter by {@code destination}: rows given up by a
 *       publisher's DROP policy (never delivered, forensics kept)
 *   <li>{@code opentmf.outbox.attempts} - summary, delivery attempts a relayed row took
 * </ul>
 *
 * <p><strong>A scrape never touches the database (1.3.0).</strong> The gauges read the last
 * SNAPSHOT, which a daemon thread ({@code opentmf-outbox-metrics}) refreshes every
 * {@code opentmf.outbox.metrics-refresh} (default 15s; the first refresh runs at start). Up to
 * 1.2.1 every scrape ran the three gauge queries and, on a multi-million-row table, one scrape
 * outlasted the scrape timeout. Before the first refresh, and after a failed one, the gauges read
 * NaN - "no value", never a misleading zero. {@code relay-lag} is computed at READ time from the
 * oldest open row's instant, so it keeps growing between refreshes.
 */
@Slf4j
class OutboxMetrics {

  static final String PENDING = "opentmf.outbox.pending";
  static final String PARKED = "opentmf.outbox.parked";
  static final String IN_FLIGHT = "opentmf.outbox.in-flight";
  static final String RELAY_LAG = "opentmf.outbox.relay-lag";
  static final String METRICS_AGE = "opentmf.outbox.metrics-age";
  static final String RELAYED = "opentmf.outbox.relayed";
  static final String DROPPED = "opentmf.outbox.dropped";
  static final String ATTEMPTS = "opentmf.outbox.attempts";
  static final String TAG_DESTINATION = "destination";

  /**
   * The last values read from the database; {@code null} instant = no open row; {@code known}
   * false = no successful refresh yet (or the last one failed): every gauge reads NaN.
   */
  record Snapshot(
      boolean known, double pending, double parked, double inFlight, Instant openSince) {

    static final Snapshot NONE = new Snapshot(false, Double.NaN, Double.NaN, Double.NaN, null);
  }

  private final MeterRegistry registry;
  private final OutboxEventRepository repository;
  private final Duration refreshEvery;
  private final AtomicReference<Snapshot> snapshot = new AtomicReference<>(Snapshot.NONE);
  private final AtomicReference<Instant> lastRefreshed = new AtomicReference<>();
  private ScheduledExecutorService refresher;

  OutboxMetrics(MeterRegistry registry, OutboxEventRepository repository, Duration refreshEvery) {
    this.registry = registry;
    this.repository = repository;
    this.refreshEvery = refreshEvery;
    Gauge.builder(PENDING, this, m -> m.snapshot.get().pending())
        .description("Outbox rows not yet relayed nor cancelled (pending, in-flight included)")
        .register(registry);
    Gauge.builder(PARKED, this, m -> m.snapshot.get().parked())
        .description("Outbox rows parked (delivery budget exhausted) - alert when > 0")
        .register(registry);
    Gauge.builder(IN_FLIGHT, this, m -> m.snapshot.get().inFlight())
        .description("Outbox rows claimed by a live lease whose send is not booked yet")
        .register(registry);
    Gauge.builder(RELAY_LAG, this, OutboxMetrics::relayLagSeconds)
        .baseUnit("seconds")
        .description("How long the oldest released pending outbox row has been deliverable")
        .register(registry);
    Gauge.builder(METRICS_AGE, this, OutboxMetrics::metricsAgeSeconds)
        .baseUnit("seconds")
        .description("Seconds since the outbox gauges were last refreshed successfully")
        .register(registry);
  }

  /** Starts the refresher: the first refresh now, then every {@code metrics-refresh}. */
  @PostConstruct
  void start() {
    refresher =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "opentmf-outbox-metrics");
              thread.setDaemon(true);
              return thread;
            });
    refresher.scheduleWithFixedDelay(
        this::refresh, 0, refreshEvery.toMillis(), TimeUnit.MILLISECONDS);
  }

  /** Stops the refresher. */
  @PreDestroy
  void stop() {
    if (refresher != null) {
      refresher.shutdownNow();
    }
  }

  /**
   * Re-reads the gauge values - four indexed queries over the OPEN rows only, each with a 5 s
   * query timeout, so a database that does not answer becomes a FAILED refresh rather than a
   * stuck one. A failure leaves NaN (no value) until the next refresh succeeds, never reaches a
   * scrape, and lets {@code metrics-age} keep growing.
   */
  void refresh() {
    try {
      long parked = repository.countParked();
      long pending = repository.countOpenNotParked() + parked;
      long inFlight = repository.countInFlight(OffsetDateTime.now(ZoneOffset.UTC));
      Instant openSince = repository.findOldestOpenSince().orElse(null);
      snapshot.set(new Snapshot(true, pending, parked, inFlight, openSince));
      lastRefreshed.set(Instant.now());
    } catch (RuntimeException ex) {
      snapshot.set(Snapshot.NONE);
      log.warn("Outbox gauges not refreshed - they read NaN until the next refresh", ex);
    }
  }

  /** Books one DROP exhaustion: the per-destination dropped counter (never the relayed one). */
  public void recordDropped(String destination) {
    registry.counter(DROPPED, TAG_DESTINATION, destination).increment();
  }

  /** Books one successful relay: increments the per-destination counter, records attempts. */
  public void recordRelayed(String destination, int attempts) {
    registry.counter(RELAYED, TAG_DESTINATION, destination).increment();
    registry.summary(ATTEMPTS).record(attempts);
  }

  /** Seconds since the last successful refresh; NaN before the first. */
  double metricsAgeSeconds() {
    Instant last = lastRefreshed.get();
    return last == null ? Double.NaN : Duration.between(last, Instant.now()).toMillis() / 1000d;
  }

  /**
   * Seconds since the oldest open row became deliverable, as of NOW (no database read); 0 when
   * no open row is deliverable yet; NaN before the first refresh.
   */
  double relayLagSeconds() {
    Snapshot last = snapshot.get();
    if (!last.known()) {
      return Double.NaN;
    }
    Instant since = last.openSince();
    if (since == null) {
      return 0d;
    }
    return Math.max(0d, Duration.between(since, Instant.now()).toMillis() / 1000d);
  }
}
