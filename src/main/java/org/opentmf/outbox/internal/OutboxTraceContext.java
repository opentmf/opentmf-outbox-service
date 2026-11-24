package org.opentmf.outbox.internal;

import java.util.Map;

/**
 * The trace context current where a row is appended, as the W3C trace-context headers the row
 * stores (1.12.0). The writer asks for it only when the caller gave the row no {@value
 * #TRACEPARENT} of its own; the relay later parents the row's {@code outbox relay} span on what was
 * stored. The library's implementation reads the application's Micrometer {@code Tracer} and
 * {@code Propagator}; it exists only where Micrometer Tracing is on the classpath.
 */
@FunctionalInterface
public interface OutboxTraceContext {

  /** The W3C trace-context header: version, trace id, parent span id and trace flags. */
  String TRACEPARENT = "traceparent";

  /** The W3C header of vendor-specific trace state that travels with {@value #TRACEPARENT}. */
  String TRACESTATE = "tracestate";

  /**
   * The current trace context as headers: {@value #TRACEPARENT}, and {@value #TRACESTATE} when
   * the context has one. Empty when no trace context is current - never null.
   */
  Map<String, String> current();
}
