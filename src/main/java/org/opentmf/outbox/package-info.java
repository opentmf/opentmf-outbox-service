/**
 * The transactional outbox as a library ({@code opentmf-outbox-service}) - the standard for
 * "DB state change + external effect must happen together". The business transaction writes its
 * state change AND one outbox row in the same local transaction
 * ({@link org.opentmf.outbox.OutboxWriter}); the in-service relay delivers the effect
 * afterwards, at-least-once, through the {@link org.opentmf.outbox.OutboxPublisher} SPI (Kafka
 * default, HTTP with per-row client-profile selection); consumers dedup via the
 * {@code x-idempotency-key} header.
 *
 * <p>State is DERIVED, no status column: pending = {@code relayed_on is null}; parked = pending
 * AND {@code attempts >= max-attempts}; relayed = {@code relayed_on is not null}.
 *
 * <p><strong>The seam is the contract:</strong> this package is the public API - the writer,
 * the maintenance service (prune / unpark / TMF630 list / inspect), the row + view, the state
 * vocabulary, the properties and the publisher SPI pair. Everything under
 * {@code org.opentmf.outbox.internal} (repository, relay, worker, publishers, backoff, trigger,
 * metrics, auto-configuration, ops controller) is implementation with no compatibility
 * promise; {@link org.opentmf.outbox.OutboxArchRules} lets consumers enforce the boundary in
 * one line.
 */
package org.opentmf.outbox;
