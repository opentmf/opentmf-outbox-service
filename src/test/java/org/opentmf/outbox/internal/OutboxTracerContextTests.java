package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxWriter;
import org.opentmf.outbox.fixture.TestTracing;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.context.ApplicationEventPublisher;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/**
 * The trace context an append stores, read through the application's Micrometer tracer and
 * propagator: the current span's W3C {@code traceparent} - sampled or not - and nothing without a
 * current span, a tracer or a propagator; tracestate kept, baggage never stored.
 */
class OutboxTracerContextTests {

  private static final ObjectMapper JSON = new ObjectMapper();

  private final TestTracing tracing = new TestTracing();

  private static <T> ObjectProvider<T> provider(Class<T> type, T bean) {
    StaticListableBeanFactory factory = new StaticListableBeanFactory();
    if (bean != null) {
      factory.addBean(type.getSimpleName(), bean);
    }
    return factory.getBeanProvider(type);
  }

  private static OutboxTracerContext context(Tracer tracer, Propagator propagator) {
    return new OutboxTracerContext(
        provider(Tracer.class, tracer), provider(Propagator.class, propagator));
  }

  /** {@code body} with {@code span} current; the span ends after it. */
  private static <T> T inSpan(Tracer tracer, Span span, Supplier<T> body) {
    try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
      return body.get();
    } finally {
      span.end();
    }
  }

  @Test
  void insideASpan_theCurrentSpansTraceparentIsGiven() {
    OutboxTracerContext context = context(tracing.tracer(), tracing.propagator());
    Span span = tracing.tracer().nextSpan().name("request").start();

    Map<String, String> fields = inSpan(tracing.tracer(), span, context::current);

    assertThat(fields).containsOnlyKeys("traceparent");
    String traceparent = fields.get("traceparent");
    assertThat(TestTracing.traceId(traceparent)).isEqualTo(span.context().traceId());
    assertThat(TestTracing.spanId(traceparent)).isEqualTo(span.context().spanId());
    assertThat(TestTracing.sampled(traceparent)).isTrue();
  }

  @Test
  void withoutACurrentSpan_nothingIsGiven() {
    assertThat(context(tracing.tracer(), tracing.propagator()).current()).isEmpty();
  }

  @Test
  void anUnsampledSpan_isGivenToo_soTheTraceIdCarriesOnWithSamplingOff() {
    TestTracing unsampled = new TestTracing(Sampler.alwaysOff());
    OutboxTracerContext context = context(unsampled.tracer(), unsampled.propagator());
    Span span = unsampled.tracer().nextSpan().name("request").start();

    Map<String, String> fields = inSpan(unsampled.tracer(), span, context::current);

    assertThat(span.context().sampled()).as("the span is not recorded").isFalse();
    String traceparent = fields.get("traceparent");
    assertThat(traceparent).isNotNull();
    assertThat(TestTracing.sampled(traceparent)).as("flags say: not sampled").isFalse();
    assertThat(TestTracing.traceId(traceparent)).isEqualTo(span.context().traceId());
    assertThat(TestTracing.spanId(traceparent)).isEqualTo(span.context().spanId());
  }

  @Test
  void withoutATracerOrAPropagator_nothingIsGiven() {
    Span span = tracing.tracer().nextSpan().name("request").start();

    Map<String, String> noTracer =
        inSpan(tracing.tracer(), span, context(null, tracing.propagator())::current);
    Map<String, String> noPropagator =
        inSpan(tracing.tracer(), span, context(tracing.tracer(), null)::current);

    assertThat(noTracer).isEmpty();
    assertThat(noPropagator).isEmpty();
  }

  /** A propagator that writes these headers, whatever the context. */
  private static Propagator writing(Map<String, String> headers) {
    Propagator propagator = mock(Propagator.class);
    doAnswer(
            invocation -> {
              Object carrier = invocation.getArgument(1);
              Propagator.Setter<Object> setter = invocation.getArgument(2);
              headers.forEach((name, value) -> setter.set(carrier, name, value));
              return null;
            })
        .when(propagator)
        .inject(any(), any(), any());
    return propagator;
  }

  private Map<String, String> currentWith(Propagator propagator) {
    Span span = tracing.tracer().nextSpan().name("request").start();
    return inSpan(tracing.tracer(), span, context(tracing.tracer(), propagator)::current);
  }

  @Test
  void tracestateIsKept_baggageAndOtherFieldsAreNot() {
    Map<String, String> fields =
        currentWith(
            writing(
                Map.of(
                    "Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                    "TraceState", "vendor=a",
                    "baggage", "user.id=42",
                    "x-other", "1")));

    assertThat(fields)
        .containsOnly(
            Map.entry("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            Map.entry("tracestate", "vendor=a"));
  }

  @Test
  void aBlankTracestate_isNotKept() {
    Map<String, String> fields =
        currentWith(writing(Map.of("traceparent", "00-a-b-01", "tracestate", " ")));

    assertThat(fields).containsOnlyKeys("traceparent");
  }

  @Test
  void aPropagatorThatWritesNoTraceparent_givesNothing() {
    assertThat(currentWith(writing(Map.of("b3", "a-b-1")))).isEmpty();
    assertThat(currentWith(writing(Map.of("traceparent", "")))).isEmpty();
  }

  @Test
  void aPropagatorThatFails_givesNothing_andDoesNotFailTheAppend() {
    Propagator failing = mock(Propagator.class);
    doThrow(new IllegalStateException("broken")).when(failing).inject(any(), any(), any());

    assertThat(currentWith(failing)).isEmpty();
  }

  // --- the writer with the real tracer: the stored row ---

  private final OutboxEventRepository repository = mock(OutboxEventRepository.class);

  private OutboxWriter writer(OutboxTraceContext traceContext) {
    when(repository.save(any()))
        .thenAnswer(
            invocation -> {
              OutboxEvent event = invocation.getArgument(0);
              event.setId(7L);
              return event;
            });
    return new OutboxWriter(
        repository,
        mock(ApplicationEventPublisher.class),
        JSON,
        () -> null,
        OutboxClock.system(),
        () -> traceContext);
  }

  private static Map<String, String> stored(OutboxEvent row) {
    return JSON.readValue(row.getHeaders(), new TypeReference<Map<String, String>>() {});
  }

  @Test
  void anAppendInsideASpan_storesThatSpansTraceIdAndSpanId() {
    OutboxWriter writer = writer(context(tracing.tracer(), tracing.propagator()));
    Span span = tracing.tracer().nextSpan().name("request").start();

    OutboxEvent row =
        inSpan(
            tracing.tracer(),
            span,
            () -> writer.append("thing", "a-1", "thing.created.v1", "topic", Map.of()));

    String traceparent = stored(row).get("traceparent");
    assertThat(TestTracing.traceId(traceparent)).isEqualTo(span.context().traceId());
    assertThat(TestTracing.spanId(traceparent)).isEqualTo(span.context().spanId());
  }

  @Test
  void anAppendWithoutASpan_storesNoHeaders() {
    OutboxWriter writer = writer(context(tracing.tracer(), tracing.propagator()));

    OutboxEvent row = writer.append("thing", "a-1", "thing.created.v1", "topic", Map.of());

    assertThat(row.getHeaders()).isNull();
  }

  @Test
  void anAppendWithItsOwnTraceparent_keepsIt_insideAnotherSpan() {
    OutboxWriter writer = writer(context(tracing.tracer(), tracing.propagator()));
    String given = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    Span span = tracing.tracer().nextSpan().name("request").start();

    OutboxEvent row =
        inSpan(
            tracing.tracer(),
            span,
            () ->
                writer.append(
                    "thing", "a-1", "thing.created.v1", "topic", Map.of(),
                    Map.of("traceparent", given)));

    assertThat(stored(row)).containsExactly(Map.entry("traceparent", given));
  }
}
