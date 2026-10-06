package org.opentmf.outbox;

/**
 * What one bounded prune call did: the terminal rows it deleted, and whether expired rows remain
 * because the call stopped on its time budget - the caller then simply calls again.
 *
 * @param relayed relayed (and dropped) rows deleted
 * @param cancelled cancelled rows deleted
 * @param moreToPrune whether the call stopped on its budget with expired rows left
 */
public record OutboxPruneResult(long relayed, long cancelled, boolean moreToPrune) {

  /** All rows deleted by the call. */
  public long pruned() {
    return relayed + cancelled;
  }
}
