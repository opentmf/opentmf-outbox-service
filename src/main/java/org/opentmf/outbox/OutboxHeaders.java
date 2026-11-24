package org.opentmf.outbox;

import java.util.UUID;

/**
 * The relay-stamped WIRE headers and the idempotency-key format - a CROSS-SERVICE contract
 * (downstream dedup tables key on {@link #idempotencyKey(String, OutboxEvent)}); made public so a
 * consumer publisher can honour "MUST attach {@code x-idempotency-key}" without hard-coding names.
 * Both built-in publishers forward every stored header and then stamp these three, REPLACING a
 * stored header of the same name. The row's {@code reference} is never a wire header.
 */
public final class OutboxHeaders {

  /**
   * The consumer's dedup key: {@code <spring.application.name>:outbox:<id>:<key_token>}, or {@code
   * <spring.application.name>:outbox:<id>} for a row without a key token (booked before 1.12.0) -
   * see {@link #idempotencyKey(String, OutboxEvent)}.
   */
  public static final String IDEMPOTENCY_KEY = "x-idempotency-key";

  /** The row's {@code event_type}. */
  public static final String EVENT_TYPE = "x-event-type";

  /** The emitting service ({@code spring.application.name}). */
  public static final String PRODUCER = "x-producer";

  private OutboxHeaders() {}

  /**
   * The idempotency key for one row (1.12.0): {@code <serviceName>:outbox:<id>:<key_token>} - the
   * row's {@code key_token}, minted at booking, makes it unique even
   * where an {@code id} repeats (a recreated table, a restore to a point in time). A row without
   * one - booked before 1.12.0 - keeps {@code <serviceName>:outbox:<id>}, exactly the key it was
   * first sent with, so a retry across the upgrade is still recognised. Both built-in publishers
   * use this; so should a consumer's own.
   */
  public static String idempotencyKey(String serviceName, OutboxEvent event) {
    UUID token = event.getKeyToken();
    String byId = keyById(serviceName, event.getId());
    return token == null ? byId : byId + ":" + token;
  }

  /**
   * The idempotency key of an {@code id} alone: {@code <serviceName>:outbox:<outboxId>}.
   *
   * @deprecated since 1.12.0 - an {@code id} repeats once the table is recreated or the database is
   *     restored to a point in time, and a consumer that already holds this key drops the new event
   *     as a duplicate. Use {@link #idempotencyKey(String, OutboxEvent)}, which builds the row's
   *     key - and this very key for a row booked before 1.12.0.
   */
  @Deprecated(since = "1.12.0")
  public static String idempotencyKey(String serviceName, long outboxId) {
    return keyById(serviceName, outboxId);
  }

  private static String keyById(String serviceName, long outboxId) {
    return "%s:outbox:%d".formatted(serviceName, outboxId);
  }
}
