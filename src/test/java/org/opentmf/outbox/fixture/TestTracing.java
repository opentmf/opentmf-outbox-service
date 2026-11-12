package org.opentmf.outbox.fixture;

import io.micrometer.observation.ObservationHandler.FirstMatchingCompositeObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.List;

/**
 * A real OpenTelemetry tracer behind Micrometer's three standard tracing handlers - grouped as
 * Spring Boot groups them, first match wins - with W3C propagation and every finished span kept in
 * memory. Its {@link #registry} is what a consumer's {@code ObservationRegistry} is once Boot's
 * tracing is on.
 */
public final class TestTracing {

  private static final String SCOPE = "opentmf-outbox-test";

  private final InMemorySpanExporter exporter = InMemorySpanExporter.create();
  private final ObservationRegistry registry = ObservationRegistry.create();

  public TestTracing() {
    SdkTracerProvider provider =
        SdkTracerProvider.builder()
            .setSampler(Sampler.alwaysOn())
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build();
    OpenTelemetrySdk sdk =
        OpenTelemetrySdk.builder()
            .setTracerProvider(provider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
    Tracer tracer =
        new OtelTracer(sdk.getTracer(SCOPE), new OtelCurrentTraceContext(), event -> {});
    Propagator propagator = new OtelPropagator(sdk.getPropagators(), sdk.getTracer(SCOPE));
    registry
        .observationConfig()
        .observationHandler(
            new FirstMatchingCompositeObservationHandler(
                new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
                new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
                new DefaultTracingObservationHandler(tracer)));
  }

  /** The registry the handlers are on. */
  public ObservationRegistry registry() {
    return registry;
  }

  /** Every span finished so far, in the order they ended. */
  public List<SpanData> spans() {
    return exporter.getFinishedSpanItems();
  }

  /** The finished span of that name; exactly one must exist. */
  public SpanData span(String name) {
    List<SpanData> named = spans().stream().filter(span -> span.getName().equals(name)).toList();
    if (named.size() != 1) {
      throw new AssertionError("expected one span named '" + name + "', finished: " + spans());
    }
    return named.get(0);
  }

  /** The trace id of a W3C {@code traceparent} ({@code 00-<trace>-<span>-<flags>}). */
  public static String traceId(String traceparent) {
    return traceparent.split("-")[1];
  }

  /** The span id of a W3C {@code traceparent}. */
  public static String spanId(String traceparent) {
    return traceparent.split("-")[2];
  }
}
