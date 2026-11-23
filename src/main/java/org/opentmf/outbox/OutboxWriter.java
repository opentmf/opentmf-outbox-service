package org.opentmf.outbox;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.opentmf.outbox.internal.OutboxAppended;
import org.opentmf.outbox.internal.OutboxClock;
import org.opentmf.outbox.internal.OutboxEventRepository;
import org.opentmf.outbox.internal.OutboxLaneStamper;
import org.opentmf.outbox.internal.OutboxTraceContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/**
 * The write side of the outbox: appends one outbox row in the CALLER's transaction
 * (propagation {@code MANDATORY} — never dual-write, never a transaction of its own). The
 * payload is serialized at write time: the event is a fact frozen at commit, not re-read later.
 *
 * <p>Since 1.3.0 the row is also stamped with the LANE and ORDERING KEY its publisher names,
 * through the relay's own router - frozen at write time like the headers. The router is
 * resolved lazily, so a publisher that itself appends through this writer creates no cycle.
 *
 * <p>Since 1.11.1 a row the caller gives no {@code traceparent} header (in any letter case) stores
 * the trace context current at the append, as the application's tracing writes it: the relay then
 * sends the event in the trace of the work that appended it. A header the caller gives is stored as
 * it is. Without a current trace context - no tracing on the classpath, no current span - nothing
 * is added. {@code opentmf.outbox.stamp-trace-context=false} turns it off.
 *
 * <p>After the insert an {@code OutboxAppended} application event is published; it fires the
 * relay nudge only AFTER the caller's transaction commits.
 */
public class OutboxWriter {

  private final OutboxEventRepository repository;
  private final ApplicationEventPublisher eventPublisher;
  private final ObjectMapper objectMapper;
  private final Supplier<OutboxLaneStamper> laneStamper;
  private final OutboxClock clock;
  private final Supplier<OutboxTraceContext> traceContext;

  /** A writer that stamps no lane - every row it appends rides ORDERED. */
  public OutboxWriter(
      OutboxEventRepository repository,
      ApplicationEventPublisher eventPublisher,
      ObjectMapper objectMapper) {
    this(repository, eventPublisher, objectMapper, () -> null);
  }

  /** The library's writer: rows are stamped by the (lazily resolved) lane stamper. */
  public OutboxWriter(
      OutboxEventRepository repository,
      ApplicationEventPublisher eventPublisher,
      ObjectMapper objectMapper,
      Supplier<OutboxLaneStamper> laneStamper) {
    this(repository, eventPublisher, objectMapper, laneStamper, OutboxClock.system());
  }

  /**
   * The library's writer on the outbox's time (1.9.0): {@code created_on} and the first due time
   * come from the database's clock, as every lease and retry does - never the pod's own.
   */
  public OutboxWriter(
      OutboxEventRepository repository,
      ApplicationEventPublisher eventPublisher,
      ObjectMapper objectMapper,
      Supplier<OutboxLaneStamper> laneStamper,
      OutboxClock clock) {
    this(repository, eventPublisher, objectMapper, laneStamper, clock, () -> null);
  }

  /**
   * The library's writer with the trace context of each append (1.11.1): a row the caller gives no
   * {@code traceparent} stores the one {@code traceContext} names as current - resolved at each
   * append; a null supplier result stores none.
   */
  public OutboxWriter(
      OutboxEventRepository repository,
      ApplicationEventPublisher eventPublisher,
      ObjectMapper objectMapper,
      Supplier<OutboxLaneStamper> laneStamper,
      OutboxClock clock,
      Supplier<OutboxTraceContext> traceContext) {
    this.repository = repository;
    this.eventPublisher = eventPublisher;
    this.objectMapper = objectMapper;
    this.laneStamper = laneStamper;
    this.clock = clock;
    this.traceContext = traceContext;
  }

  /**
   * Appends an outbox row without extra headers. The relay stamps the relay headers
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
   * Appends an outbox row with additional headers frozen at write time (e.g.
   * {@code x-schema-version}). Relay-stamped headers win over same-named stored ones.
   */
  @Transactional(propagation = Propagation.MANDATORY)
  public OutboxEvent append(
      String aggregateType, String aggregateId, String eventType, String destination,
      Object payload, Map<String, String> headers) {
    return doAppend(aggregateType, aggregateId, eventType, destination, null, payload, headers);
  }

  /**
   * Appends an HTTP-destined row selecting a NAMED client profile for delivery (the
   * per-row selector). {@code clientProfile} null = the HTTP publisher's own
   * resolution; ignored entirely for Kafka destinations.
   */
  @Transactional(propagation = Propagation.MANDATORY)
  public OutboxEvent append(
      String aggregateType, String aggregateId, String eventType, String destination,
      String clientProfile, Object payload, Map<String, String> headers) {
    return doAppend(
        aggregateType, aggregateId, eventType, destination, clientProfile, payload, headers);
  }

  /**
   * The request-shaped append - the ONE entry that says everything, including the
   * scheduled-send HOLD ({@link OutboxAppend#withReleaseAt}): a row with a future
   * {@code releaseAt} is not claimable before that instant (null or past = deliverable now).
   * The positional overloads above are conveniences over this.
   */
  @Transactional(propagation = Propagation.MANDATORY)
  public OutboxEvent append(OutboxAppend request) {
    return doAppend(request);
  }

  /** Un-annotated on purpose: the overloads delegate here, never to each other (proxy rule). */
  private OutboxEvent doAppend(
      String aggregateType, String aggregateId, String eventType, String destination,
      String clientProfile, Object payload, Map<String, String> headers) {
    return doAppend(
        OutboxAppend.of(aggregateType, aggregateId, eventType, destination, payload)
            .withClientProfile(clientProfile)
            .withHeaders(headers));
  }

  private OutboxEvent doAppend(OutboxAppend request) {
    OutboxEvent event = new OutboxEvent();
    event.setAggregateType(request.aggregateType());
    event.setAggregateId(request.aggregateId());
    event.setEventType(request.eventType());
    event.setDestination(request.destination());
    event.setClientProfile(request.clientProfile());
    event.setPayload(objectMapper.writeValueAsString(request.payload()));
    Map<String, String> headers = withTraceContext(request.headers());
    event.setHeaders(headers.isEmpty() ? null : objectMapper.writeValueAsString(headers));
    OffsetDateTime now = clock.now();
    event.setCreatedOn(now);
    event.setNextAttemptOn(now);
    event.setReleaseAt(request.releaseAt());
    event.setReference(request.reference());
    event.setKeyToken(UUID.randomUUID()); // the row's own part of its idempotency key (1.11.1)
    OutboxLaneStamper stamper = laneStamper.get();
    if (stamper != null) {
      stamper.stamp(event);
    }
    OutboxEvent saved = repository.save(event);
    eventPublisher.publishEvent(new OutboxAppended(saved.getId(), saved.getLane()));
    return saved;
  }

  /**
   * The caller's headers, plus the current trace context when the caller gave no {@code
   * traceparent}; a header the caller gave is never replaced.
   */
  private Map<String, String> withTraceContext(Map<String, String> given) {
    OutboxTraceContext source = traceContext.get();
    if (source == null || named(given, OutboxTraceContext.TRACEPARENT)) {
      return given;
    }
    Map<String, String> current = source.current();
    if (current.isEmpty()) {
      return given;
    }
    Map<String, String> stamped = new LinkedHashMap<>(given);
    current.forEach(
        (name, value) -> {
          if (!named(stamped, name)) {
            stamped.put(name, value);
          }
        });
    return stamped;
  }

  /** Whether {@code headers} has a header of that name, in any letter case. */
  private static boolean named(Map<String, String> headers, String name) {
    return headers.keySet().stream().anyMatch(name::equalsIgnoreCase);
  }
}
