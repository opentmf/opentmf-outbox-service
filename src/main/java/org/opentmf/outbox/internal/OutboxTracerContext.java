package org.opentmf.outbox.internal;

import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;

/**
 * The library's {@link OutboxTraceContext}: the application's Micrometer {@link Tracer} gives the
 * current trace context, and the application's {@link Propagator} writes it - the same propagator
 * that writes the headers of every request the application sends, so the stored value is exactly
 * what the application would have sent at that moment. An unsampled context is written too (its
 * trace flags say so): the trace id carries on with sampling off.
 *
 * <p>Only {@value #TRACEPARENT} and {@value #TRACESTATE} are kept from what the propagator
 * writes. Baggage is not stored: it is the application's own data, which the row would keep in the
 * table until the prune and every built-in publisher would forward as a header of the event, where
 * the application never chose to send it. A caller that wants baggage on the event passes it as a
 * header. A propagator that writes no {@value #TRACEPARENT} (B3 propagation only) gives nothing to
 * store: the relay continues a W3C context only.
 *
 * <p>Both beans are looked up at each append, so the order in which the application's tracing is
 * configured does not matter; without either one nothing is stored. A propagator that fails is
 * logged and stores nothing - tracing never fails an append.
 */
@Slf4j
final class OutboxTracerContext implements OutboxTraceContext {

  private final ObjectProvider<Tracer> tracer;
  private final ObjectProvider<Propagator> propagator;

  OutboxTracerContext(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
    this.tracer = tracer;
    this.propagator = propagator;
  }

  @Override
  public Map<String, String> current() {
    Tracer currentTracer = tracer.getIfUnique();
    Propagator currentPropagator = propagator.getIfUnique();
    if (currentTracer == null || currentPropagator == null) {
      return Map.of();
    }
    TraceContext context = currentTracer.currentTraceContext().context();
    if (context == null) {
      return Map.of();
    }
    Map<String, String> written = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    try {
      currentPropagator.inject(context, written, Map::put);
    } catch (RuntimeException ex) {
      log.warn("The trace context could not be written; the row stores none", ex);
      return Map.of();
    }
    String traceparent = written.get(TRACEPARENT);
    if (traceparent == null || traceparent.isBlank()) {
      return Map.of();
    }
    Map<String, String> fields = new LinkedHashMap<>();
    fields.put(TRACEPARENT, traceparent);
    String tracestate = written.get(TRACESTATE);
    if (tracestate != null && !tracestate.isBlank()) {
      fields.put(TRACESTATE, tracestate);
    }
    return fields;
  }
}
