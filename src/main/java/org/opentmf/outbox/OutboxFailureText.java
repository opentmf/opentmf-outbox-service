package org.opentmf.outbox;

/**
 * The text a failed attempt stores as its row's {@code last_error} (1.12.0). A failure's own
 * message can quote what a service would rather keep out of its rows and logs - an HTTP receiver's
 * response body, an address with its path and query, a value a peer echoed - and the stored text is
 * served by the ops surface and printed in the relay's retry, park, drop and cancel log lines. A
 * consumer that declares a bean of this type gives that text itself; without one, {@link #DEFAULT}
 * stores exactly what every release before 1.12.0 stored.
 *
 * <p>The relay calls the bean inside the booking transaction of the failed attempt, on the lane's
 * thread, with a read-only {@link Row} - the stored row itself is never handed out. Around the call
 * the library keeps its own duties: it cuts the text to the column's bound; a null or blank text,
 * or a bean that throws, stores the failure's type name alone (a throw is logged with the bean's
 * exception type, not its message) - the booking is never rolled back by this bean.
 *
 * <p>The ops views without their sensitive text ({@code opentmf.outbox.ops.sensitive-in-views=
 * false}) show a row's failure KIND, read back from the stored text's leading throwable name before
 * its first {@code :}. A text that does not start so shows as {@code unclassified} there; {@link
 * #typed} is the way to keep the kind.
 */
@FunctionalInterface
public interface OutboxFailureText {

  /**
   * The text up to 1.11.0: the failure's simple type name, {@code ": "} and its own message; for a
   * failure without a message its {@code toString()} (its binary type name).
   */
  OutboxFailureText DEFAULT =
      (row, failure) ->
          failure.getMessage() == null ? failure.toString() : typed(failure, failure.getMessage());

  /**
   * The text to store for this failed attempt of {@code row}.
   *
   * @param row the failed row's facts, read-only; {@code attempts} counts this attempt
   * @param failure what the attempt threw (an {@code Error} arrives wrapped in {@link
   *     OutboxSendError})
   * @return the text; null or blank stores the failure's type name alone
   */
  String describe(Row row, RuntimeException failure);

  /**
   * {@code text} behind the failure's simple type name and {@code ": "} - the shape whose kind the
   * ops views read back; the type name alone for a null {@code text}.
   */
  static String typed(Throwable failure, String text) {
    String kind = failure.getClass().getSimpleName();
    return text == null ? kind : kind + ": " + text;
  }

  /**
   * A failed row's facts as the relay hands them to {@link #describe}: no payload, no stored
   * headers, no way back to the row.
   *
   * @param id the row's id
   * @param aggregateType the row's aggregate type
   * @param aggregateId the row's aggregate id
   * @param eventType the row's event type
   * @param destination the row's destination - a topic, or a URL as appended
   * @param attempts the failed attempts so far, this one included
   */
  record Row(
      Long id,
      String aggregateType,
      String aggregateId,
      String eventType,
      String destination,
      int attempts) {}
}
