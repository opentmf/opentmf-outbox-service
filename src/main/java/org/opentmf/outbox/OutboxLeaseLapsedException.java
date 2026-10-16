package org.opentmf.outbox;

/**
 * The failure a LAPSED lease stands for (1.9.0): the previous holder's send was under way when its
 * lease ran out unbooked - its pod died mid-send, or the send outran its bound (the larger of its
 * lease and {@code send-timeout}; the send heartbeat renews no further). The send may have been
 * delivered (the idempotency key makes the receiver's dedup trivial); it was never booked.
 *
 * <p>The next claim of the row books this as a failed attempt - the publisher's backoff, then its
 * budget parks or drops the row - and does not send on that claim. Up to 1.8.0 a lapse was never
 * counted: a send that always outlived its lease was re-sent every lease, forever. A booking hook
 * sees it as the booking's {@link OutboxBooking#failure()}.
 */
public class OutboxLeaseLapsedException extends RuntimeException {

  public OutboxLeaseLapsedException(String message) {
    super(message);
  }
}
