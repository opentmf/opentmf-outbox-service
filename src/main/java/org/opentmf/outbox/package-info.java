/**
 * The S23 transactional outbox as a library ({@code opentmf-outbox-service}, extracted
 * 2026-08-26 from dnms-email-adapter's sealed module per the estate ruling) - the standard for
 * "DB state change + external effect must happen together". The business transaction writes its
 * state change AND one outbox row in the same local transaction
 * ({@link org.opentmf.outbox.OutboxWriter}); the in-service relay delivers the effect
 * afterwards, at-least-once, through the {@link org.opentmf.outbox.OutboxPublisher} SPI (Kafka
 * default, HTTP with per-row client-profile selection); consumers dedup via the S14
 * {@code x-idempotency-key}.
 *
 * <p>State is DERIVED, no status column: pending = {@code relayed_on is null}; parked = pending
 * AND {@code attempts >= max-attempts}; relayed = {@code relayed_on is not null}.
 *
 * <p><strong>The seam is the contract:</strong> public types are the writer, the maintenance
 * service (prune / unpark / TMF630 list / inspect), the row + view, the state vocabulary, the
 * publisher SPI pair and the ops controller. Repository, relay, worker, publishers, backoff,
 * trigger, metrics and properties are package-private;
 * {@link org.opentmf.outbox.OutboxArchRules} lets consumers enforce the seal in one line.
 */
package org.opentmf.outbox;
