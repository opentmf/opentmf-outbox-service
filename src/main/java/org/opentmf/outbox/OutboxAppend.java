package org.opentmf.outbox;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Everything one {@link OutboxWriter#append(OutboxAppend)} call can say - the request shape
 * that keeps the writer's API from growing an overload per optional selector. Build it from
 * the five mandatory facts with {@link #of} and add the optional ones with the withers.
 *
 * @param aggregateType aggregate kind, e.g. {@code party-interaction}
 * @param aggregateId aggregate identity - becomes the Kafka message key
 * @param eventType payload event type, e.g. {@code comm.outcome.v1}
 * @param destination a Kafka topic name, or an {@code http(s)://} URL for HTTP delivery
 * @param clientProfile optional named HTTP client profile; null = the publisher's own
 *     resolution; ignored for Kafka destinations
 * @param payload the event FACT OBJECT, serialized at write time
 * @param headers extra WIRE headers frozen at write time, never null (empty = none) - both
 *     built-in publishers forward all of them
 * @param releaseAt the scheduled-send HOLD - the row is not claimable before this instant;
 *     null = deliverable now. Frozen at write time: the retry backoff never moves it, and there
 *     is no reschedule API - cancel and re-append instead
 * @param reference optional PRIVATE correlation (e.g. a subscription id): filterable on the ops
 *     list, on the row view, never on the wire
 */
public record OutboxAppend(
    String aggregateType,
    String aggregateId,
    String eventType,
    String destination,
    String clientProfile,
    Object payload,
    Map<String, String> headers,
    OffsetDateTime releaseAt,
    String reference) {

  /** The mandatory facts; no client profile, no extra headers, no hold, no reference. */
  public static OutboxAppend of(
      String aggregateType, String aggregateId, String eventType, String destination,
      Object payload) {
    return new OutboxAppend(
        aggregateType, aggregateId, eventType, destination, null, payload, Map.of(), null,
        null);
  }

  /** Selects a NAMED HTTP client profile for delivery. */
  public OutboxAppend withClientProfile(String profile) {
    return new OutboxAppend(
        aggregateType, aggregateId, eventType, destination, profile, payload, headers,
        releaseAt, reference);
  }

  /** Extra wire headers frozen at write time (relay-stamped headers win on name collisions). */
  public OutboxAppend withHeaders(Map<String, String> extraHeaders) {
    return new OutboxAppend(
        aggregateType, aggregateId, eventType, destination, clientProfile, payload,
        extraHeaders == null ? Map.of() : Map.copyOf(extraHeaders), releaseAt, reference);
  }

  /** The scheduled-send hold: not deliverable before {@code instant}; null = no hold. */
  public OutboxAppend withReleaseAt(OffsetDateTime instant) {
    return new OutboxAppend(
        aggregateType, aggregateId, eventType, destination, clientProfile, payload, headers,
        instant, reference);
  }

  /** A private correlation value (max 128 chars) - never forwarded to the wire. */
  public OutboxAppend withReference(String privateReference) {
    return new OutboxAppend(
        aggregateType, aggregateId, eventType, destination, clientProfile, payload, headers,
        releaseAt, privateReference);
  }
}
