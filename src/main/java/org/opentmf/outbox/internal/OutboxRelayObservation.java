package org.opentmf.outbox.internal;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.ReceiverContext;
import java.util.Map;
import java.util.TreeMap;
import org.opentmf.outbox.OutboxEvent;

/**
 * The relay span (1.11.1, OUTBOX-SEND-OBSERVATION-PARENT-1): a send whose row stored a W3C {@code
 * traceparent} runs inside a receiver-kind observation that takes the stored trace context as its
 * parent. The send's own observation - Spring Kafka's template observation, or the consumer's
 * {@code RestClient} observation on an HTTP profile client - opens while this one is current, so
 * the producer or client span becomes its child and the {@code traceparent} that leaves is that
 * child's: the event a consumer reads joins the trace of the request that appended it. Up to 1.11.0
 * the send's observation found none current on a relay thread and started a new trace, replacing
 * the stored header.
 *
 * <p>In a trace view the span is named {@value #SPAN_NAME}; it covers one send attempt, from the
 * moment the relay hands the row to its publisher to the publisher's verdict, and its start after
 * the stored span's shows how long the row waited in the outbox. A retried row gets one relay span
 * per attempt, all in the same trace. Micrometer's meter handler, when registered, also records the
 * observation as the timer {@value #NAME} - for rows that stored a {@code traceparent} only.
 *
 * <p>A row without a stored {@code traceparent}, or a registry with no handlers, opens nothing: the
 * send behaves exactly as before. A malformed stored value gives the propagator no parent, so the
 * relay span itself is a root.
 */
final class OutboxRelayObservation {

  /** The observation's name, and the timer's when a meter handler is registered. */
  static final String NAME = "opentmf.outbox.relay";

  /** The span's name in a trace view. */
  static final String SPAN_NAME = "outbox relay";

  /** The W3C trace-context header whose presence on the row opens the relay span. */
  static final String TRACEPARENT = "traceparent";

  /** No registry: every send runs as it did before 1.11.1. */
  static final OutboxRelayObservation NONE = new OutboxRelayObservation(ObservationRegistry.NOOP);

  private final ObservationRegistry registry;

  OutboxRelayObservation(ObservationRegistry registry) {
    this.registry = registry;
  }

  /**
   * Runs {@code send} inside the relay span when the row stored a {@code traceparent}; otherwise
   * runs it as it is. The span is stopped when the send returns or fails - an interrupted send
   * included - and a failure is recorded on it before it propagates unchanged.
   */
  void around(
      OutboxEvent event, String transport, Map<String, String> storedHeaders, Runnable send) {
    Map<String, String> carrier = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    carrier.putAll(storedHeaders);
    if (registry.isNoop() || !carrier.containsKey(TRACEPARENT)) {
      send.run();
      return;
    }
    ReceiverContext<Map<String, String>> context = new ReceiverContext<>(Map::get);
    context.setCarrier(carrier);
    Observation.createNotStarted(NAME, () -> context, registry)
        .contextualName(SPAN_NAME)
        .lowCardinalityKeyValue("outbox.transport", transport)
        .highCardinalityKeyValue("outbox.id", String.valueOf(event.getId()))
        .highCardinalityKeyValue("outbox.event-type", event.getEventType())
        .highCardinalityKeyValue("outbox.destination", event.getDestination())
        .observe(send);
  }
}
