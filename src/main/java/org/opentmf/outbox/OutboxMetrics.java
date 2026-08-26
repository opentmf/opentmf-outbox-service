package org.opentmf.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.OffsetDateTime;

/**
 * The S23 mandatory outbox meter family under the LIBRARY-STABLE names ({@code opentmf.outbox.*}
 * - one name across every consumer; the emitting service is distinguished by the registry's
 * common tags / scrape identity, never by a per-service metric prefix):
 *
 * <ul>
 *   <li>{@code opentmf.outbox.pending} - gauge, rows not yet relayed
 *   <li>{@code opentmf.outbox.parked} - gauge, alert when above 0
 *   <li>{@code opentmf.outbox.relay-lag} - gauge, age of the oldest pending row (seconds)
 *   <li>{@code opentmf.outbox.relayed} - counter by {@code destination} (closed tag set)
 *   <li>{@code opentmf.outbox.attempts} - summary, delivery attempts a relayed row took
 * </ul>
 */
class OutboxMetrics {

  static final String PENDING = "opentmf.outbox.pending";
  static final String PARKED = "opentmf.outbox.parked";
  static final String RELAY_LAG = "opentmf.outbox.relay-lag";
  static final String RELAYED = "opentmf.outbox.relayed";
  static final String ATTEMPTS = "opentmf.outbox.attempts";
  static final String TAG_DESTINATION = "destination";

  private final MeterRegistry registry;
  private final OutboxEventRepository repository;

  OutboxMetrics(
      MeterRegistry registry, OutboxEventRepository repository, OutboxProperties properties) {
    this.registry = registry;
    this.repository = repository;
    Gauge.builder(PENDING, repository, OutboxEventRepository::countByRelayedOnIsNull)
        .description("Outbox rows not yet relayed (pending = relayed_on is null)")
        .register(registry);
    Gauge.builder(
            PARKED,
            repository,
            r -> r.countByRelayedOnIsNullAndAttemptsGreaterThanEqual(properties.getMaxAttempts()))
        .description("Outbox rows parked at max-attempts - alert when > 0")
        .register(registry);
    Gauge.builder(RELAY_LAG, this, OutboxMetrics::relayLagSeconds)
        .baseUnit("seconds")
        .description("Age of the oldest pending outbox row")
        .register(registry);
  }

  /** Books one successful relay: increments the per-destination counter, records attempts. */
  public void recordRelayed(String destination, int attempts) {
    registry.counter(RELAYED, TAG_DESTINATION, destination).increment();
    registry.summary(ATTEMPTS).record(attempts);
  }

  double relayLagSeconds() {
    return repository
        .findOldestPendingCreatedOn()
        .map(o -> Math.max(0d, Duration.between(o, OffsetDateTime.now()).toMillis() / 1000d))
        .orElse(0d);
  }
}
