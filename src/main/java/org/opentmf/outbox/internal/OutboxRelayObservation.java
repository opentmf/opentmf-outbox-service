package org.opentmf.outbox.internal;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.ReceiverContext;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.opentmf.outbox.OutboxEvent;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/**
 * The relay span (1.12.0, OUTBOX-SEND-OBSERVATION-PARENT-1): a row that stored a W3C {@code
 * traceparent} is relayed inside a receiver-kind observation that takes the stored trace context as
 * its parent. The send's own observation - Spring Kafka's template observation, or the consumer's
 * {@code RestClient} observation on an HTTP profile client - opens while this one is current, so
 * the producer or client span becomes its child and the {@code traceparent} that leaves is that
 * child's: the event a consumer reads joins the trace of the request that appended it. Up to 1.11.0
 * the send's observation found none current on a relay thread and started a new trace, replacing
 * the stored header.
 *
 * <p>The span covers the row's whole ROUND on its lane's thread: the renewal of a lease that waited
 * in the hand-off queue, the send, and the booking transaction with its hook and listeners - so
 * every statement of that round that the application traces is a span of the request's trace. Two
 * numbers tell what came before it: {@value #CLAIM_MS}, how long the claim that took the row ran,
 * and {@value #QUEUED_MS}, the wait from that claim's commit to the start of the send. The claim
 * itself is not traced: an idle pod's sweeps claim nothing and open nothing. A retried row gets one
 * relay span per round, all in the same trace. Micrometer's meter handler, when registered, also
 * records the observation as the timer {@value #NAME} - for rows that stored a {@code traceparent}
 * only.
 *
 * <p>A row without a stored {@code traceparent}, or a registry with no handlers, opens nothing: the
 * round behaves exactly as before. A malformed stored value gives the propagator no parent, so the
 * relay span itself is a root. No attribute carries the payload, a header's value or a path: the
 * destination is a Kafka topic as it is, a URL as its scheme, host and port only.
 */
final class OutboxRelayObservation {

  /** The observation's name, and the timer's when a meter handler is registered. */
  static final String NAME = "opentmf.outbox.relay";

  /** The span's name in a trace view. */
  static final String SPAN_NAME = "outbox relay";

  /** The W3C trace-context header whose presence on the row opens the relay span. */
  static final String TRACEPARENT = "traceparent";

  /** The low-cardinality transport: {@code kafka}, {@code http}, a publisher's own, or other. */
  static final String TRANSPORT_KEY = "outbox.transport";

  /** How long the claim that took the row ran, in milliseconds. */
  static final String CLAIM_MS = "outbox.claim.ms";

  /** From the claim's commit to the start of the send, in milliseconds. */
  static final String QUEUED_MS = "outbox.queued.ms";

  /** The transport of a publisher that names none (a consumer's own publisher). */
  static final String OTHER_TRANSPORT = "other";

  /** No registry: every send runs as it did before 1.12.0. */
  static final OutboxRelayObservation NONE = new OutboxRelayObservation(ObservationRegistry.NOOP);

  private static final ObjectMapper HEADERS = new ObjectMapper();
  private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {};

  private final ObservationRegistry registry;

  OutboxRelayObservation(ObservationRegistry registry) {
    this.registry = registry;
  }

  /** The context of one row's round: which row, so the publisher's send opens no second span. */
  private static final class RoundContext extends ReceiverContext<Map<String, String>> {
    private final long rowId;

    RoundContext(long rowId, Map<String, String> carrier) {
      super(Map::get);
      this.rowId = rowId;
      setCarrier(carrier);
    }
  }

  /**
   * Runs one row's round - {@code round} is its renewal, send and booking - inside the relay span
   * when the row stored a {@code traceparent}; otherwise runs it as it is. The span is stopped when
   * the round returns or throws; a failure the round throws is recorded on it before it propagates
   * unchanged, and a send's failure the round books is recorded by {@link #sendFailed}.
   *
   * @param transport the publisher's transport, or {@code null}: the library's publishers name
   *     theirs when they send
   * @param claimNanos how long the claim that took the row ran
   */
  void round(OutboxEvent event, String transport, long claimNanos, Runnable round) {
    Map<String, String> carrier = registry.isNoop() ? Map.of() : carrier(event.getHeaders());
    if (!carrier.containsKey(TRACEPARENT)) {
      round.run();
      return;
    }
    RoundContext context = new RoundContext(event.getId(), carrier);
    Observation.createNotStarted(NAME, () -> context, registry)
        .contextualName(SPAN_NAME)
        .lowCardinalityKeyValue(TRANSPORT_KEY, transport != null ? transport : OTHER_TRANSPORT)
        .highCardinalityKeyValue("outbox.id", String.valueOf(event.getId()))
        .highCardinalityKeyValue("outbox.event-type", String.valueOf(event.getEventType()))
        .highCardinalityKeyValue("outbox.destination", destination(event.getDestination()))
        .highCardinalityKeyValue(CLAIM_MS, millis(claimNanos))
        .observe(round);
  }

  /**
   * The send of row {@code event} starts now: records on its round's span how long the row waited
   * since its claim committed. Nothing when no round span of that row is current.
   */
  void sendStarts(OutboxEvent event, long sinceClaimNanos) {
    Observation round = roundOf(event);
    if (round != null) {
      round.highCardinalityKeyValue(QUEUED_MS, millis(sinceClaimNanos));
    }
  }

  /**
   * The send of row {@code event} failed with {@code failure}: recorded on its round's span, which
   * then ends in error though the round books the failure and returns. Nothing when no round span
   * of that row is current.
   */
  void sendFailed(OutboxEvent event, Throwable failure) {
    Observation round = roundOf(event);
    if (round != null) {
      round.error(failure);
    }
  }

  /**
   * Runs {@code send} - a library publisher's send of {@code event} - inside the relay span. Within
   * the row's round the round's span is that span: the send names its transport on it, and no
   * second span opens. Called outside a round (a publisher used on its own), it opens the span for
   * the send alone when the row stored a {@code traceparent}; otherwise runs it as it is.
   */
  void around(
      OutboxEvent event, String transport, Map<String, String> storedHeaders, Runnable send) {
    Observation round = roundOf(event);
    if (round != null) {
      round.lowCardinalityKeyValue(TRANSPORT_KEY, transport);
      send.run();
      return;
    }
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
        .lowCardinalityKeyValue(TRANSPORT_KEY, transport)
        .highCardinalityKeyValue("outbox.id", String.valueOf(event.getId()))
        .highCardinalityKeyValue("outbox.event-type", String.valueOf(event.getEventType()))
        .highCardinalityKeyValue("outbox.destination", destination(event.getDestination()))
        .observe(send);
  }

  /** The current observation when it is the round span of row {@code event}, else null. */
  private Observation roundOf(OutboxEvent event) {
    Observation current = registry.getCurrentObservation();
    if (current != null
        && current.getContext() instanceof RoundContext round
        && event.getId() != null
        && round.rowId == event.getId()) {
      return current;
    }
    return null;
  }

  /**
   * The stored headers as a case-blind carrier; none when they are absent or unreadable - the
   * publisher then fails the send on them as before, outside any span.
   */
  private static Map<String, String> carrier(String headers) {
    Map<String, String> carrier = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    if (headers == null || !headers.toLowerCase(Locale.ROOT).contains(TRACEPARENT)) {
      return carrier;
    }
    try {
      Map<String, String> stored = HEADERS.readValue(headers, STRING_MAP);
      stored.forEach(
          (name, value) -> {
            if (name != null && value != null) {
              carrier.put(name, value);
            }
          });
    } catch (JacksonException ex) {
      carrier.clear();
    }
    return carrier;
  }

  /**
   * A destination as a span may carry it: a URL as {@code scheme://host[:port]} - no user, path,
   * query or fragment - and anything else (a Kafka topic) as it is.
   */
  static String destination(String destination) {
    if (destination == null || !destination.contains("://")) {
      return String.valueOf(destination);
    }
    try {
      URI uri = URI.create(destination);
      if (uri.getScheme() == null || uri.getHost() == null) {
        return "unparsed";
      }
      return uri.getScheme()
          + "://"
          + uri.getHost()
          + (uri.getPort() >= 0 ? ":" + uri.getPort() : "");
    } catch (IllegalArgumentException ex) {
      return "unparsed";
    }
  }

  /** Nanoseconds as milliseconds with three decimals. */
  private static String millis(long nanos) {
    return String.format(Locale.ROOT, "%.3f", Math.max(0, nanos) / 1_000_000.0);
  }
}
