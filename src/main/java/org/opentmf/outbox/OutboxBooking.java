package org.opentmf.outbox;

import org.opentmf.outbox.OutboxPublisher.ExhaustionOutcome;

/**
 * What the relay booked for one delivery attempt - handed to the row's publisher
 * ({@link OutboxPublisher#onBooked}) INSIDE the short booking transaction that writes the row's
 * own bookkeeping, so a publisher's database write commits (or rolls back) together with it.
 * Since 1.3.0 the send itself runs in no transaction: this is where a publisher's database
 * write belongs.
 *
 * @param outcome what the relay booked
 * @param result what {@link OutboxPublisher#deliver} returned (RELAYED only; may be null)
 * @param failure why the attempt failed (every outcome but RELAYED): the publisher's own
 *     exception, or the refusal of a booking hook or relayed listener
 * @param exhaustion PARK or DROP for {@link Outcome#EXHAUSTED}, else null
 */
public record OutboxBooking(
    Outcome outcome, Object result, RuntimeException failure, ExhaustionOutcome exhaustion) {

  /** The outcome of one attempt, as booked on the row. */
  public enum Outcome {
    /** Delivered: {@code relayed_on} is stamped in this transaction. */
    RELAYED,
    /** Failed: {@code attempts++}, the next attempt is scheduled by the publisher's backoff. */
    RETRY,
    /**
     * Failed for the last time (budget spent, or a {@link TerminalOutboxException}): the row is
     * parked or dropped - {@link #exhaustion} says which.
     */
    EXHAUSTED,
    /**
     * Failed, and the row was cancelled while the send still ran (after its lease lapsed - a live
     * lease refuses the cancel): it retires cancelled - no retry, no park.
     */
    CANCELLED
  }

  /** A RELAYED booking carrying the publisher's delivery result. */
  public static OutboxBooking relayed(Object result) {
    return new OutboxBooking(Outcome.RELAYED, result, null, null);
  }

  /** A failure booking - RETRY, EXHAUSTED (with its exhaustion) or CANCELLED. */
  public static OutboxBooking failed(
      Outcome outcome, RuntimeException failure, ExhaustionOutcome exhaustion) {
    return new OutboxBooking(outcome, null, failure, exhaustion);
  }
}
