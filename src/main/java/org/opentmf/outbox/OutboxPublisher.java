package org.opentmf.outbox;

/**
 * The publisher SPI - the effect seam (per-row {@code destination}, OPTIONAL per-row
 * {@code clientProfile}). The library ships a Kafka implementation (topic
 * destinations, the default) and an HTTP implementation ({@code http(s)://} destinations with
 * named-client selection); a consumer may contribute its own bean for anything else
 * at-least-once - the relay routes each row to the FIRST publisher that supports it.
 *
 * <p>Contract: {@link #publish} either delivers the effect or throws - a normal return is the
 * relay's license to stamp {@code relayed_on}. Idempotency rides the
 * {@code x-idempotency-key} header ({@code <service>:outbox:<id>}); implementations MUST attach it.
 */
public interface OutboxPublisher {

  /** Whether this publisher handles the row's destination. First match wins (bean order). */
  boolean supports(OutboxEvent event);

  /** Delivers the effect or throws (the relay then books the failure + backoff). */
  void publish(OutboxEvent event);
}
