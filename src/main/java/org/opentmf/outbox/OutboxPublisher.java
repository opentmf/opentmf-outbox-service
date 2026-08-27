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
 * <p>Contract: {@link #publish} either delivers the effect or throws - a normal return is the
 * relay's license to stamp {@code relayed_on}. Idempotency rides the
 * {@link OutboxHeaders#IDEMPOTENCY_KEY} header ({@link OutboxHeaders#idempotencyKey}); a
 * publisher MUST attach it. A publisher runs on the single relay thread INSIDE the claim
 * transaction and MAY write to the same database there (e.g. mark a business record as
 * recorded) - such writes commit with the batch, together with {@code relayed_on}.
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

  /** Delivers the effect or throws (the relay then books the failure by this policy). */
  void publish(OutboxEvent event);

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
