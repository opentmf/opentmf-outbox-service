package org.opentmf.outbox;

import java.time.Duration;

/**
 * The publisher SPI - the effect seam (per-row {@code destination}, OPTIONAL per-row
 * {@code clientProfile}). The library ships a Kafka implementation (topic
 * destinations, the default) and an HTTP implementation ({@code http(s)://} destinations with
 * named-client selection); a consumer may contribute its own bean for anything else
 * at-least-once - the relay routes each row to the FIRST publisher that supports it (bean
 * order; {@code @Order} a consumer publisher ahead of the library defaults).
 *
 * <p>Contract: {@link #deliver} (by default: {@link #publish}) either delivers the effect or
 * throws - a normal return is the relay's license to stamp {@code relayed_on}. Idempotency rides
 * the {@link OutboxHeaders#IDEMPOTENCY_KEY} header ({@link OutboxHeaders#idempotencyKey}); a
 * publisher MUST attach it.
 *
 * <p><strong>Lanes and the lease (1.3.0).</strong> Each row rides the {@link #lane} its publisher
 * names. {@link Lane#ORDERED} (the default) is the single relay thread, rows in {@code id} order;
 * {@link Lane#CONCURRENT} sends rows in parallel under {@code opentmf.outbox.concurrent
 * .max-in-flight}, independent of each other except where {@link #orderingKey} ties them. Either
 * way the row is CLAIMED BY LEASE: a short claim transaction stamps {@code claimed_until}, the
 * send runs in NO transaction and holds no database connection, and a short booking transaction
 * writes the outcome - guarded on the stamped lease, so a holder whose lease lapsed books
 * nothing. The {@link #lease} must exceed the publisher's longest call.
 *
 * <p><strong>A publisher's own database write</strong> (e.g. marking a business record as
 * recorded) belongs in {@link #onBooked}, which runs inside the booking transaction for EVERY
 * outcome and commits with the row's bookkeeping. Until 1.2.x such a write could live in
 * {@code publish} itself, inside the claim transaction - since 1.3.0 {@code publish} runs in no
 * transaction, so a write there commits on its own (or fails for want of a transaction).
 *
 * <p><strong>Failure policy (per publisher, 1.2.0):</strong> a thrown {@link RuntimeException}
 * is a RETRY - the relay books {@code attempts++}, {@code last_error} and the next attempt by
 * {@link #backoff}, until {@link #maxAttempts} is reached; then {@link #onExhausted} decides
 * between PARK (ops action needed) and DROP (give up, keep the forensics). A publisher may
 * throw {@link TerminalOutboxException} to reach the exhaustion outcome IMMEDIATELY (an answer
 * that says retrying is pointless). The defaults reproduce the library-wide behaviour, so an
 * existing publisher is unaffected.
 */
public interface OutboxPublisher {

  /** The relay lane a row rides. */
  enum Lane {
    /**
     * The single relay thread, rows in {@code id} order (a failed row backs off and does not
     * stop the rows behind it) - the 1.2.x behaviour, the default.
     */
    ORDERED,
    /**
     * Rows sent in parallel, one per (virtual, where enabled) thread, at most
     * {@code opentmf.outbox.concurrent.max-in-flight} at a time. The library promises NO order
     * between them, nor against ORDERED rows - except that rows sharing an {@link #orderingKey}
     * are never in flight together and are taken in {@code id} order.
     */
    CONCURRENT
  }

  /** What happens to a row whose delivery attempts are exhausted. */
  enum ExhaustionOutcome {
    /**
     * The row stays pending with {@code parked_on} stamped: unclaimable, never auto-pruned,
     * the {@code parked} gauge alerts, {@code unpark} is the explicit ops action.
     */
    PARK,
    /**
     * The row is given up: {@code relayed_on} stamped so it leaves the pending set,
     * {@code last_error} kept for forensics, the {@code opentmf.outbox.dropped} counter
     * incremented, a WARN logged. Relayed listeners are NOT fired and
     * {@code opentmf.outbox.relayed} is NOT incremented - nothing was delivered.
     */
    DROP
  }

  /** Whether this publisher handles the row's destination. First match wins (bean order). */
  boolean supports(OutboxEvent event);

  /**
   * Delivers the effect or throws (the relay then books the failure by this policy). Runs in NO
   * transaction since 1.3.0 - see {@link #onBooked} for a database write.
   */
  void publish(OutboxEvent event);

  /**
   * The relay's entry point: delivers the effect and returns a result for {@link #onBooked}, or
   * throws. The default calls {@link #publish} and returns {@code null}; a publisher whose
   * booking needs something from the answer (an id the receiver assigned) overrides this, and
   * its {@code publish} may simply delegate here.
   *
   * @return anything the booking hook needs, or {@code null}
   */
  default Object deliver(OutboxEvent event) {
    publish(event);
    return null;
  }

  /**
   * The booking hook: called for EVERY booked attempt of this publisher's rows - relayed,
   * failed-will-retry, exhausted, cancelled - INSIDE the short booking transaction, after the
   * row's own bookkeeping is set on the managed entity and (for RELAYED) before the
   * {@link OutboxRelayedListener}s. A database write here commits with the row's bookkeeping and
   * rolls back with it. A throw rolls the booking back: a refused RELAYED booking is then booked
   * as an ordinary failure (the hook is called again with that outcome); a refused failure
   * booking books nothing and the row is redelivered once its lease lapses.
   *
   * @param event the row, managed - {@code relayedOn} / {@code attempts} / {@code parkedOn} as
   *     just booked
   * @param booking what was booked, with the delivery result or the failure
   */
  default void onBooked(OutboxEvent event, OutboxBooking booking) {}

  /** The lane this row rides; the default is {@link Lane#ORDERED}. */
  default Lane lane(OutboxEvent event) {
    return Lane.ORDERED;
  }

  /**
   * CONCURRENT lane only: rows with the same non-null key are never in flight together and are
   * taken in {@code id} order - in order on the happy path (a failed row backs off, and later
   * rows of its key may pass it). {@code null} (the default) = independent of every other row.
   */
  default String orderingKey(OutboxEvent event) {
    return null;
  }

  /**
   * How long a claim of this row lasts - it MUST exceed this publisher's longest call, or a
   * second holder re-claims the row while the first is still sending.
   *
   * @return the lease, or {@code null} for the library default of the row's lane
   *     ({@code opentmf.outbox.lease} for CONCURRENT, {@code opentmf.outbox.ordered.lease} for
   *     ORDERED)
   */
  default Duration lease(OutboxEvent event) {
    return null;
  }

  /**
   * Delivery attempts before {@link #onExhausted} applies, for this row.
   *
   * @return the budget, or {@code 0} for the library default ({@code opentmf.outbox.max-attempts})
   */
  default int maxAttempts(OutboxEvent event) {
    return 0;
  }

  /**
   * Delay before the next attempt after a failure, for this row.
   *
   * @param attempt the failed-attempt count including the one just booked (at least 1)
   * @return the delay, or {@code null} for the library's exponential backoff
   */
  default Duration backoff(OutboxEvent event, int attempt) {
    return null;
  }

  /** The exhaustion outcome for this row; the library default is {@link ExhaustionOutcome#PARK}. */
  default ExhaustionOutcome onExhausted(OutboxEvent event) {
    return ExhaustionOutcome.PARK;
  }
}
