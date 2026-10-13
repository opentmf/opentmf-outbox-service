package org.opentmf.outbox.internal;

import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxPublisher.Lane;

/**
 * Application event published when an outbox row is appended (or unparked) inside a business
 * transaction. {@code OutboxRelayTrigger} receives it AFTER COMMIT and pokes the relay - the
 * normal-path latency is milliseconds; the sweep remains the safety net.
 *
 * @param outboxId the appended {@link OutboxEvent} id, or {@code 0} for an unpark by filter
 *     (tracing only - the relay always claims from the table, never from this event)
 * @param lane the row's stamped lane; {@code null} = ORDERED, or not known (an unpark by filter)
 *     - either way the relay's next pass claims the ORDERED lane too (1.6.0)
 */
public record OutboxAppended(long outboxId, Lane lane) {}
