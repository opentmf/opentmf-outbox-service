package org.opentmf.outbox;

import java.time.OffsetDateTime;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/**
 * The write side of the §23 outbox: appends one outbox row in the CALLER's transaction
 * (propagation {@code MANDATORY} — never dual-write, never a transaction of its own). The
 * payload is serialized at write time: the event is a fact frozen at commit, not re-read later.
 *
 * <p>After the insert an {@code OutboxAppended} application event is published; it fires the
 * relay nudge only AFTER the caller's transaction commits.
 */
@RequiredArgsConstructor
public class OutboxWriter {

  private final OutboxEventRepository repository;
  private final ApplicationEventPublisher eventPublisher;
  private final ObjectMapper objectMapper;

  /**
   * Appends an outbox row without extra headers. The relay stamps the §14 relay headers
   * ({@code x-idempotency-key}, {@code x-event-type}, {@code x-producer}) at publish time.
   *
   * @param aggregateType aggregate kind, e.g. {@code party-interaction}
   * @param aggregateId aggregate identity — becomes the Kafka message key
   * @param eventType payload event type, e.g. {@code comm.outcome.v1}
   * @param destination a Kafka topic name, or an {@code http(s)://} URL for HTTP delivery
   * @param payload the event FACT OBJECT (serialized here; do not pass pre-serialized JSON — a
   *     {@code String} would be serialized as a JSON string)
   * @return the persisted row (id assigned)
   */
  @Transactional(propagation = Propagation.MANDATORY)
  public OutboxEvent append(
      String aggregateType, String aggregateId, String eventType, String destination,
      Object payload) {
    return doAppend(aggregateType, aggregateId, eventType, destination, null, payload, Map.of());
  }

  /**
   * Appends an outbox row with additional §14 headers frozen at write time (e.g.
   * {@code x-schema-version}). Relay-stamped headers win over same-named stored ones.
   */
  @Transactional(propagation = Propagation.MANDATORY)
  public OutboxEvent append(
      String aggregateType, String aggregateId, String eventType, String destination,
      Object payload, Map<String, String> headers) {
    return doAppend(aggregateType, aggregateId, eventType, destination, null, payload, headers);
  }

  /**
   * Appends an HTTP-destined row selecting a NAMED client profile for delivery (the 2026-08-26
   * hub ruling's per-row selector). {@code clientProfile} null = the HTTP publisher's own
   * resolution; ignored entirely for Kafka destinations.
   */
  @Transactional(propagation = Propagation.MANDATORY)
  public OutboxEvent append(
      String aggregateType, String aggregateId, String eventType, String destination,
      String clientProfile, Object payload, Map<String, String> headers) {
    return doAppend(
        aggregateType, aggregateId, eventType, destination, clientProfile, payload, headers);
  }

  /** Un-annotated on purpose: the overloads delegate here, never to each other (proxy rule). */
  private OutboxEvent doAppend(
      String aggregateType, String aggregateId, String eventType, String destination,
      String clientProfile, Object payload, Map<String, String> headers) {
    OutboxEvent event = new OutboxEvent();
    event.setAggregateType(aggregateType);
    event.setAggregateId(aggregateId);
    event.setEventType(eventType);
    event.setDestination(destination);
    event.setClientProfile(clientProfile);
    event.setPayload(objectMapper.writeValueAsString(payload));
    event.setHeaders(headers.isEmpty() ? null : objectMapper.writeValueAsString(headers));
    event.setCreatedOn(OffsetDateTime.now());
    event.setNextAttemptOn(OffsetDateTime.now());
    OutboxEvent saved = repository.save(event);
    eventPublisher.publishEvent(new OutboxAppended(saved.getId()));
    return saved;
  }
}
