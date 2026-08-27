package org.opentmf.outbox.internal;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * The outbox meter family under the LIBRARY-STABLE names ({@code opentmf.outbox.*}
 * - one name across every consumer; the emitting service is distinguished by the registry's
 * common tags / scrape identity, never by a per-service metric prefix):
 *
 * <ul>
 *   <li>{@code opentmf.outbox.pending} - gauge, rows not yet relayed nor cancelled (held rows
 *       included)
 *   <li>{@code opentmf.outbox.parked} - gauge, alert when above 0
 *   <li>{@code opentmf.outbox.relay-lag} - gauge, how long the oldest RELEASED pending row has
 *       been deliverable (seconds) - a held row is not lagging until its hold passes
 *   <li>{@code opentmf.outbox.relayed} - counter by {@code destination} (closed tag set)
 *   <li>{@code opentmf.outbox.dropped} - counter by {@code destination}: rows given up by a
 *       publisher's DROP policy (never delivered, forensics kept)
 *   <li>{@code opentmf.outbox.attempts} - summary, delivery attempts a relayed row took
 * </ul>
 */
class OutboxMetrics {

  static final String PENDING = "opentmf.outbox.pending";
  static final String PARKED = "opentmf.outbox.parked";
  static final String RELAY_LAG = "opentmf.outbox.relay-lag";
  static final String RELAYED = "opentmf.outbox.relayed";
  static final String DROPPED = "opentmf.outbox.dropped";
  static final String ATTEMPTS = "opentmf.outbox.attempts";
  static final String TAG_DESTINATION = "destination";

  private final MeterRegistry registry;
  private final OutboxEventRepository repository;

  OutboxMetrics(MeterRegistry registry, OutboxEventRepository repository) {
    this.registry = registry;
    this.repository = repository;
    Gauge.builder(
            PENDING, repository, OutboxEventRepository::countByRelayedOnIsNullAndCancelledOnIsNull)
        .description("Outbox rows not yet relayed nor cancelled (pending)")
        .register(registry);
    Gauge.builder(
            PARKED,
            repository,
            OutboxEventRepository::countByRelayedOnIsNullAndCancelledOnIsNullAndParkedOnIsNotNull)
        .description("Outbox rows parked (delivery budget exhausted) - alert when > 0")
        .register(registry);
    Gauge.builder(RELAY_LAG, this, OutboxMetrics::relayLagSeconds)
        .baseUnit("seconds")
        .description("How long the oldest released pending outbox row has been deliverable")
        .register(registry);
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

  double relayLagSeconds() {
    OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
    return repository
        .findOldestPendingSince(now)
        .map(since -> Math.max(0d, Duration.between(since, now).toMillis() / 1000d))
        .orElse(0d);
  }
}
