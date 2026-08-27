/**
 * The transactional outbox as a library ({@code opentmf-outbox-service}) - the standard for
 * "DB state change + external effect must happen together". The business transaction writes its
 * state change AND one outbox row in the same local transaction
 * ({@link org.opentmf.outbox.OutboxWriter}); the in-service relay delivers the effect
 * afterwards, at-least-once, through the {@link org.opentmf.outbox.OutboxPublisher} SPI (Kafka
 * default, HTTP with per-row client-profile selection); consumers dedup via the
 * {@link org.opentmf.outbox.OutboxHeaders#IDEMPOTENCY_KEY} header.
 *
 * <p>State is DERIVED, no status column: pending = {@code relayed_on is null and cancelled_on
 * is null} (a pending row with a future {@code release_at} is HELD); parked = pending AND
 * {@code parked_on is not null}; relayed = {@code relayed_on is not null}; cancelled =
 * {@code cancelled_on is not null}.
 *
 * <p><strong>The seam is the contract:</strong> this package is the public API -
 * {@link org.opentmf.outbox.OutboxWriter} + {@link org.opentmf.outbox.OutboxAppend} (the write
 * side), {@link org.opentmf.outbox.OutboxMaintenanceService} (prune / unpark / cancel / TMF630
 * list / inspect), {@link org.opentmf.outbox.OutboxEvent} +
 * {@link org.opentmf.outbox.OutboxRowView} (the row and its read model), {@link org.opentmf.outbox.OutboxStateFilter} (the state
 * vocabulary), {@link org.opentmf.outbox.OutboxProperties}, the SPI types
 * {@link org.opentmf.outbox.OutboxPublisher} (+ {@link org.opentmf.outbox.TerminalOutboxException})
 * and {@link org.opentmf.outbox.OutboxClientProfileResolver}, the post-relay seam
 * {@link org.opentmf.outbox.OutboxRelayedListener}, the wire constants
 * {@link org.opentmf.outbox.OutboxHeaders}, and the seal rule
 * {@link org.opentmf.outbox.OutboxArchRules}. Everything under
 * {@code org.opentmf.outbox.internal} (repository, relay, worker, publishers, backoff, trigger,
 * metrics, auto-configuration, ops controller) is implementation with no compatibility
 * promise.
 */
package org.opentmf.outbox;
