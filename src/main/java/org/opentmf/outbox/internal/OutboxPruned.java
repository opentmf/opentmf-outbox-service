package org.opentmf.outbox.internal;

/**
 * Application event published by the retention prune after each batch that COMMITTED and deleted
 * rows (1.11.1). {@code OutboxMetrics} adds them to the {@code opentmf.outbox.pruned} counter, so
 * the rows a call deleted are counted even when a later batch of the same call fails and the call
 * itself answers with an exception, not with its result.
 *
 * @param state the deleted rows' state, {@link #RELAYED} or {@link #CANCELLED} - the kind of ROW
 *     pruned, not an outcome of the prune
 * @param rows the rows the batch deleted, above 0
 */
public record OutboxPruned(String state, int rows) {

  /** Relayed rows, pruned by {@code relayed_on}. */
  public static final String RELAYED = "relayed";

  /** Cancelled rows, pruned by {@code cancelled_on}. */
  public static final String CANCELLED = "cancelled";
}
