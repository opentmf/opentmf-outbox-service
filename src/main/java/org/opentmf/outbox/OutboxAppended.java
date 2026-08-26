package org.opentmf.outbox;

/**
 * Application event published when an outbox row is appended (or unparked) inside a business
 * transaction. {@code OutboxRelayTrigger} receives it AFTER COMMIT and pokes the relay - the
 * normal-path latency is milliseconds; the sweep remains the safety net.
 *
 * @param outboxId the appended {@link OutboxEvent} id (tracing only - the relay always claims
 *     from the table, never from this event)
 */
record OutboxAppended(long outboxId) {}
