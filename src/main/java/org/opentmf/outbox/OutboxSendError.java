package org.opentmf.outbox;

/**
 * An {@link Error} - or a checked exception thrown past the compiler - that a publisher, a booking
 * hook or a relayed listener threw, as the failure of the attempt (1.9.0). The relay books it like
 * any failure (backoff, then the publisher's budget), the thrown one as the cause. Up to 1.8.0 an
 * {@code Error} escaped the relay: it killed the sending thread, booked nothing, and the row was
 * re-sent every lease, forever.
 */
public class OutboxSendError extends RuntimeException {

  public OutboxSendError(Throwable thrown) {
    super(thrown.getClass().getSimpleName() + ": " + thrown.getMessage(), thrown);
  }
}
