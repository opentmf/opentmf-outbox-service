package org.opentmf.outbox;

import java.time.OffsetDateTime;

/**
 * Read-model of one outbox row for the ops surface (the no-direct-DB principle: what an
 * operator would otherwise SELECT in production rides an admin REST endpoint). {@code parked}
 * is the S23 DERIVED state made explicit. {@code payload} rides only the single-row inspect
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
    int attempts,
    boolean parked,
    OffsetDateTime createdOn,
    OffsetDateTime nextAttemptOn,
    OffsetDateTime relayedOn,
    String lastError,
    String payload) {

  static OutboxRowView of(OutboxEvent event, int parkThreshold, boolean withPayload) {
    return new OutboxRowView(
        event.getId(),
        event.getAggregateType(),
        event.getAggregateId(),
        event.getEventType(),
        event.getDestination(),
        event.getClientProfile(),
        event.getAttempts(),
        event.getRelayedOn() == null && event.getAttempts() >= parkThreshold,
        event.getCreatedOn(),
        event.getNextAttemptOn(),
        event.getRelayedOn(),
        event.getLastError(),
        withPayload ? event.getPayload() : null);
  }
}
