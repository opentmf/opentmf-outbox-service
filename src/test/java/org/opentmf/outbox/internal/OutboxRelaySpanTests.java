package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.fixture.TestTracing;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.mock.MockProducerFactory;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.ObjectMapper;

/**
 * OUTBOX-SEND-OBSERVATION-PARENT-1: a row that stored a W3C {@code traceparent} is sent inside the
 * relay span, a child in the stored trace, so the send's own observation - Spring Kafka's template
 * observation, a profile {@code RestClient}'s - stamps a header in that trace instead of a new
 * root's. A row without one is sent as before. The template and the client are real; only the
 * transport underneath is mocked, and the tracer is a real OpenTelemetry one ({@link TestTracing}).
 */
class OutboxRelaySpanTests {

  static final String TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";
  static final String STORED_SPAN = "00f067aa0ba902b7";
  static final String STORED = "00-" + TRACE + "-" + STORED_SPAN + "-01";

  private final TestTracing tracing = new TestTracing();
  private final List<MockProducer<Object, Object>> producers = new ArrayList<>();

  private static OutboxEvent event(String destination, String headers) {
    OutboxEvent event = new OutboxEvent();
    event.setId(11L);
    event.setAggregateId("agg-11");
    event.setEventType("dispatch.requested.v1");
    event.setDestination(destination);
    event.setPayload("{}");
    event.setHeaders(headers);
    return event;
  }

  private static String stored(String traceparent) {
    return "{\"traceparent\":\"" + traceparent + "\",\"x-custom\":\"kept\"}";
  }

  /**
   * A Kafka template as a consumer has it with {@code observation-enabled}, over a mock producer.
   */
  private KafkaOutboxPublisher kafka(boolean transactional) {
    return kafka(transactional, null);
  }

  /** As above; a transactional producer's commit fails with {@code commitFailure} when given. */
  private KafkaOutboxPublisher kafka(boolean transactional, RuntimeException commitFailure) {
    Serializer<Object> serializer =
        (topic, data) -> String.valueOf(data).getBytes(StandardCharsets.UTF_8);
    Supplier<MockProducer<Object, Object>> newProducer =
        () -> {
          MockProducer<Object, Object> producer =
              new MockProducer<>(true, null, serializer, serializer);
          producers.add(producer);
          return producer;
        };
    // the two-argument factory is always transaction-capable, the one-argument one never
    MockProducerFactory<Object, Object> factory =
        transactional
            ? new MockProducerFactory<>(
                (tx, id) -> {
                  MockProducer<Object, Object> producer = newProducer.get();
                  producer.initTransactions();
                  producer.commitTransactionException = commitFailure;
                  return producer;
                },
                "tx-")
            : new MockProducerFactory<>(newProducer);
    KafkaTemplate<Object, Object> template = new KafkaTemplate<>(factory);
    template.setObservationEnabled(true);
    template.setObservationRegistry(tracing.registry());
    return new KafkaOutboxPublisher(
        template,
        new OutboxProperties(),
        new ObjectMapper(),
        "svc",
        new OutboxRelayObservation(tracing.registry()));
  }

  private String sentTraceparent() {
    ProducerRecord<Object, Object> sent = producers.get(0).history().get(0);
    Header header = sent.headers().lastHeader("traceparent");
    assertThat(sent.headers().headers("traceparent"))
        .as("one traceparent on the record")
        .hasSize(1);
    return new String(header.value(), StandardCharsets.UTF_8);
  }

  private SpanData spanOfKind(SpanKind kind) {
    List<SpanData> spans = tracing.spans().stream().filter(s -> s.getKind() == kind).toList();
    assertThat(spans).as("one %s span among %s", kind, tracing.spans()).hasSize(1);
    return spans.get(0);
  }

  @ParameterizedTest(name = "transactional={0}")
  @ValueSource(booleans = {false, true})
  void aStoredTraceparent_isTheParentOfTheRelaySpan_andTheRecordCarriesTheProducerSpan(
      boolean transactional) {
    kafka(transactional).publish(event("dispatch.v1", stored(STORED)));

    String sent = sentTraceparent();
    assertThat(TestTracing.traceId(sent)).as("the stored trace continues").isEqualTo(TRACE);
    assertThat(TestTracing.spanId(sent)).as("not the stored span itself").isNotEqualTo(STORED_SPAN);

    SpanData relay = tracing.span(OutboxRelayObservation.SPAN_NAME);
    SpanData producer = spanOfKind(SpanKind.PRODUCER);
    assertThat(relay.getKind()).isEqualTo(SpanKind.CONSUMER);
    assertThat(relay.getParentSpanId()).as("relay span <- stored span").isEqualTo(STORED_SPAN);
    assertThat(producer.getParentSpanId())
        .as("producer span <- relay span")
        .isEqualTo(relay.getSpanId());
    assertThat(producer.getSpanId())
        .as("the header that leaves")
        .isEqualTo(TestTracing.spanId(sent));
    assertThat(relay.getAttributes().asMap().toString())
        .contains("outbox.id=11", "outbox.transport=kafka", "outbox.destination=dispatch.v1");
  }

  @Test
  void aRowWithoutATraceparent_opensNoRelaySpan_andTheRecordStartsATraceAsBefore() {
    kafka(false).publish(event("dispatch.v1", "{\"x-custom\":\"kept\"}"));

    String sent = sentTraceparent();
    assertThat(TestTracing.traceId(sent)).isNotEqualTo(TRACE);
    assertThat(tracing.spans()).as("the producer span alone").hasSize(1);
    SpanData producer = spanOfKind(SpanKind.PRODUCER);
    assertThat(producer.getParentSpanContext().isValid()).as("a root, as in 1.11.0").isFalse();
  }

  /**
   * With a meter handler on the registry - as Boot's metrics auto-configuration registers one - the
   * relay span is also a timer, tagged by the low-cardinality transport (and the handler's own
   * {@code error}) only: the row id, event type and destination stay on the span.
   */
  @Test
  void theRelaySpansTimer_carriesNoHighCardinalityTag() {
    SimpleMeterRegistry meters = new SimpleMeterRegistry();
    tracing
        .registry()
        .observationConfig()
        .observationHandler(new DefaultMeterObservationHandler(meters));

    kafka(false).publish(event("dispatch.v1", stored(STORED)));

    Timer timer = meters.get(OutboxRelayObservation.NAME).timer();
    assertThat(timer.count()).isEqualTo(1);
    assertThat(timer.getId().getTags())
        .extracting(Tag::getKey)
        .containsExactlyInAnyOrder("outbox.transport", "error");
    assertThat(timer.getId().getTag("outbox.transport")).isEqualTo("kafka");
    assertThat(tracing.span(OutboxRelayObservation.SPAN_NAME).getAttributes().asMap().toString())
        .contains("outbox.id=11", "outbox.event-type=dispatch.requested.v1");
  }

  @Test
  void aStoredHeaderNameIsMatchedWithoutCase() {
    kafka(false).publish(event("dispatch.v1", "{\"Traceparent\":\"" + STORED + "\"}"));

    assertThat(tracing.span(OutboxRelayObservation.SPAN_NAME).getParentSpanId())
        .isEqualTo(STORED_SPAN);
  }

  @Test
  void aMalformedStoredTraceparent_makesTheRelaySpanARoot() {
    kafka(false).publish(event("dispatch.v1", stored("not-a-traceparent")));

    SpanData relay = tracing.span(OutboxRelayObservation.SPAN_NAME);
    assertThat(relay.getParentSpanContext().isValid()).isFalse();
    assertThat(spanOfKind(SpanKind.PRODUCER).getParentSpanId()).isEqualTo(relay.getSpanId());
    assertThat(TestTracing.traceId(sentTraceparent())).isEqualTo(relay.getTraceId());
  }

  @Test
  void aFailedSend_endsTheRelaySpanInError_andFailsAsBefore() {
    KafkaOutboxPublisher publisher = kafka(true, new IllegalStateException("coordinator gone"));
    OutboxEvent event = event("dispatch.v1", stored(STORED));

    assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> publisher.publish(event))
        .withStackTraceContaining("coordinator gone");

    SpanData relay = tracing.span(OutboxRelayObservation.SPAN_NAME);
    assertThat(relay.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
    assertThat(relay.hasEnded()).isTrue();
  }

  /** A consumer's profile client with its observation on, against a mock server. */
  private HttpOutboxPublisher http(RestClient.Builder builder) {
    RestClient observed = builder.observationRegistry(tracing.registry()).build();
    return new HttpOutboxPublisher(
        RestClient.create(),
        (profile, destination) -> observed,
        new ObjectMapper(),
        "svc",
        new OutboxRelayObservation(tracing.registry()));
  }

  @Test
  void anObservedProfileClient_sendsTheStoredTraceOn_asItsClientSpansChild() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    server
        .expect(requestTo("https://hub/cb"))
        .andExpect(
            header("traceparent", allOf(startsWith("00-" + TRACE + "-"), not(equalTo(STORED)))))
        .andExpect(header("x-custom", "kept"))
        .andRespond(withSuccess());

    http(builder).publish(event("https://hub/cb", stored(STORED)));

    server.verify();
    SpanData relay = tracing.span(OutboxRelayObservation.SPAN_NAME);
    SpanData client = spanOfKind(SpanKind.CLIENT);
    assertThat(relay.getParentSpanId()).isEqualTo(STORED_SPAN);
    assertThat(client.getParentSpanId()).isEqualTo(relay.getSpanId());
    assertThat(relay.getAttributes().asMap().toString()).contains("outbox.transport=http");
  }

  @Test
  void anObservedProfileClient_withoutAStoredTraceparent_startsATraceAsBefore() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    server
        .expect(requestTo("https://hub/cb"))
        .andExpect(header("traceparent", not(startsWith("00-" + TRACE + "-"))))
        .andRespond(withSuccess());

    http(builder).publish(event("https://hub/cb", "{\"x-custom\":\"kept\"}"));

    server.verify();
    assertThat(tracing.spans()).as("the client span alone").hasSize(1);
    assertThat(spanOfKind(SpanKind.CLIENT).getParentSpanContext().isValid()).isFalse();
  }

  @Test
  void aFailedPost_endsTheRelaySpanInError() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    server.expect(requestTo("https://hub/cb")).andRespond(withServerError());

    HttpOutboxPublisher publisher = http(builder);
    OutboxEvent event = event("https://hub/cb", stored(STORED));
    assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> publisher.publish(event));

    SpanData relay = tracing.span(OutboxRelayObservation.SPAN_NAME);
    assertThat(relay.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
  }
}
