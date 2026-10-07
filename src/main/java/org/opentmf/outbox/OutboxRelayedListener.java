package org.opentmf.outbox;

/**
 * Post-relay hook: consumer bookkeeping that must commit atomically with {@code relayed_on} -
 * e.g. stamping a business record's state, so the effect's book entry and the stamp commit or
 * repeat together. Register any number of beans; the relay invokes them in bean order right
 * after the row's effect is delivered, INSIDE the transaction that stamps {@code relayed_on} -
 * on BOTH lanes. Since 1.3.0 that is the short booking transaction (the send itself runs in no
 * transaction), after the publisher's own {@link OutboxPublisher#onBooked} hook.
 *
 * <p>A thrown exception rolls the booking back - the stamp, the publisher's hook writes and
 * every listener's writes with it - and the row is booked as an ordinary delivery failure
 * (attempts++, backoff, park or drop at the publisher's budget): the publish then REPEATS, so
 * the destination must dedup via the {@code x-idempotency-key} exactly as for any at-least-once
 * redelivery. A listener that keeps failing exhausts the row only after the budget's worth of
 * REPUBLISHES - so a listener must be idempotent and reliable, not merely fast. Keep
 * implementations same-database and fast: the booking holds the row lock while they run.
 *
 * <p>Without this seam, per-destination bookkeeping forces a decorating {@link OutboxPublisher}
 * that must reach the library's package-private default publishers by BEAN NAME — an
 * undocumented contract a refactor would silently break. This interface is that contract,
 * made public.
 */
@FunctionalInterface
public interface OutboxRelayedListener {

  /**
   * Called once per successfully delivered row, inside the booking transaction, with
   * {@code relayedOn} already set on the (managed) entity.
   *
   * @param event the delivered row — mutating it participates in the booking transaction
   */
  void onRelayed(OutboxEvent event);
}
