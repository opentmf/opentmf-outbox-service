package org.opentmf.outbox;

import java.time.OffsetDateTime;

/**
 * Read-model of one outbox row for the ops surface (the no-direct-DB principle: what an
 * operator would otherwise SELECT in production rides an admin REST endpoint). {@code parked}
 * is the DERIVED state made explicit ({@code parkedOn} set and not cancelled); {@code releaseAt}
 * (the scheduled-send hold), {@code parkedOn}, {@code cancelledOn} and the private
 * {@code reference} ride as the raw facts. {@code payload} rides only the single-row inspect
 * (null in lists) - the list exists for triage, not bulk export; the TMF630 fields param can
 * narrow further.
 */
public record OutboxRowView(
    long id,
    String aggregateType,
    String aggregateId,
    String eventType,
    String destination,
    String clientProfile,
    String reference,
    int attempts,
    boolean parked,
    OffsetDateTime createdOn,
    OffsetDateTime nextAttemptOn,
    OffsetDateTime releaseAt,
    OffsetDateTime parkedOn,
    OffsetDateTime relayedOn,
    OffsetDateTime cancelledOn,
    String lastError,
    String payload) {

  static OutboxRowView of(OutboxEvent event, boolean withPayload) {
    return new OutboxRowView(
        event.getId(),
        event.getAggregateType(),
        event.getAggregateId(),
        event.getEventType(),
        event.getDestination(),
        event.getClientProfile(),
        event.getReference(),
        event.getAttempts(),
        event.getParkedOn() != null && event.getCancelledOn() == null,
        event.getCreatedOn(),
        event.getNextAttemptOn(),
        event.getReleaseAt(),
        event.getParkedOn(),
        event.getRelayedOn(),
        event.getCancelledOn(),
        event.getLastError(),
        withPayload ? event.getPayload() : null);
  }
}
