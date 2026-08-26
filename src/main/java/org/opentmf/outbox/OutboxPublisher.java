package org.opentmf.outbox;

/**
 * The publisher SPI - the S23 effect seam (2026-08-26 ruling: per-row {@code destination},
 * OPTIONAL per-row {@code clientProfile}). The library ships a Kafka implementation (topic
 * destinations, the default) and an HTTP implementation ({@code http(s)://} destinations with
 * named-client selection); a consumer may contribute its own bean for anything else
 * at-least-once - the relay routes each row to the FIRST publisher that supports it.
 *
 * <p>Contract: {@link #publish} either delivers the effect or throws - a normal return is the
 * relay's license to stamp {@code relayed_on}. Idempotency rides the S14
 * {@code x-idempotency-key} ({@code <service>:outbox:<id>}); implementations MUST attach it.
 */
public interface OutboxPublisher {

  /** Whether this publisher handles the row's destination. First match wins (bean order). */
  boolean supports(OutboxEvent event);

  /** Delivers the effect or throws (the relay then books the failure + backoff). */
  void publish(OutboxEvent event);
}
