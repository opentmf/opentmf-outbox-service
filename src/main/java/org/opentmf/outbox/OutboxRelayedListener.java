package org.opentmf.outbox;

/**
 * Post-relay hook: consumer bookkeeping that must ride
 * the SAME claim transaction as the delivery — e.g. stamping a business record's state
 * atomically with {@code relayed_on}, so the effect and its book entry commit or repeat
 * together. Register any number of beans; the relay invokes them in bean order right after the
 * row's effect is delivered, INSIDE the claim transaction, before {@code relayed_on} commits.
 *
 * <p>A thrown exception books an ordinary delivery failure on the row ({@code relayed_on} is
 * cleared, attempts++, backoff, park at max) — the publish then REPEATS, so the destination
 * must dedup via the {@code x-idempotency-key} exactly as for any at-least-once redelivery. A
 * listener that keeps failing parks the row only after {@code max-attempts} REPUBLISHES of the
 * effect - so a listener must be idempotent and reliable, not merely fast. Keep implementations
 * same-database and fast: this runs on the single relay thread, and its
 * writes share the claim transaction by design.
 *
 * <p>Without this seam, per-destination bookkeeping forces a decorating {@link OutboxPublisher}
 * that must reach the library's package-private default publishers by BEAN NAME — an
 * undocumented contract a refactor would silently break. This interface is that contract,
 * made public.
 */
@FunctionalInterface
public interface OutboxRelayedListener {

  /**
   * Called once per successfully delivered row, inside the claim transaction, with
   * {@code relayedOn} already set on the (managed) entity.
   *
   * @param event the delivered row — mutating it participates in the claim transaction
   */
  void onRelayed(OutboxEvent event);
}
