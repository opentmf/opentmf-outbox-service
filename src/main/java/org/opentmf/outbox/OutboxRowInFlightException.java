package org.opentmf.outbox;

import java.time.OffsetDateTime;

/**
 * The row is IN FLIGHT: a relay claimed it under a live lease and its publisher may be delivering
 * it right now, so it cannot be cancelled (1.3.1). An {@link IllegalStateException} - the ops
 * surface answers 409, like every other "not in the state the action needs" - with its own name,
 * so a caller can tell "try again once the send is booked" from "already relayed". Once the send
 * is booked the row is either relayed (no cancel any more) or back to pending (cancellable); a
 * lease that lapses unbooked frees it as well, at {@link #claimedUntil()} at the latest.
 */
public class OutboxRowInFlightException extends IllegalStateException {

  private final transient OffsetDateTime claimedUntil;

  /**
   * @param outboxId the row
   * @param claimedUntil the end of the live lease
   */
  public OutboxRowInFlightException(long outboxId, OffsetDateTime claimedUntil) {
    super(
        "Outbox row %d is in flight (leased until %s) - its send may be delivering now; cancel"
            .formatted(outboxId, claimedUntil)
            + " again once it is booked");
    this.claimedUntil = claimedUntil;
  }

  /** The end of the live lease: the row is not cancellable before its send is booked or this. */
  public OffsetDateTime claimedUntil() {
    return claimedUntil;
  }
}
