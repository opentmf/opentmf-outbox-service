package org.opentmf.outbox;

/**
 * The relay-stamped WIRE headers and the idempotency-key format - a CROSS-SERVICE contract
 * (downstream dedup tables key on {@link #idempotencyKey}); made public so a consumer publisher
 * can honour "MUST attach {@code x-idempotency-key}" without hard-coding names. Both built-in
 * publishers forward every stored header and then stamp these three, REPLACING a stored header
 * of the same name. The row's {@code reference} is never a wire header.
 */
public final class OutboxHeaders {

  /** {@code <spring.application.name>:outbox:<id>} - the consumer's dedup key. */
  public static final String IDEMPOTENCY_KEY = "x-idempotency-key";

  /** The row's {@code event_type}. */
  public static final String EVENT_TYPE = "x-event-type";

  /** The emitting service ({@code spring.application.name}). */
  public static final String PRODUCER = "x-producer";

  private OutboxHeaders() {}

  /** The idempotency key for one row: {@code <serviceName>:outbox:<outboxId>}. */
  public static String idempotencyKey(String serviceName, long outboxId) {
    return "%s:outbox:%d".formatted(serviceName, outboxId);
  }
}
