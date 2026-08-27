package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxProperties;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;

/** Relay headers stamped, key = aggregateId, ack awaited, failures unwind to the relay. */
class KafkaOutboxPublisherTests {

  @SuppressWarnings("unchecked")
  private final KafkaTemplate<Object, Object> template = mock(KafkaTemplate.class);

  private final KafkaOutboxPublisher publisher =
      new KafkaOutboxPublisher(template, new OutboxProperties(), new ObjectMapper(), "svc");

  private static OutboxEvent event(String destination, String headers) {
    OutboxEvent event = new OutboxEvent();
    event.setId(7L);
    event.setAggregateId("agg-1");
    event.setEventType("e.v1");
    event.setDestination(destination);
    event.setPayload("{}");
    event.setHeaders(headers);
    event.setReference("subscription-42"); // private - must never reach the wire
    return event;
  }

  @Test
  void supports_everyNonHttpDestination() {
    assertThat(publisher.supports(event("comm.delivery.v1", null))).isTrue();
    assertThat(publisher.supports(event("https://hub/cb", null))).isFalse();
    assertThat(publisher.supports(event("http://hub/cb", null))).isFalse();
  }

  @Test
  @SuppressWarnings("unchecked")
  void publish_stampsTheRelayHeaders_overStoredOnes() {
    when(template.isTransactional()).thenReturn(false);
    when(template.send(any(ProducerRecord.class)))
        .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

    publisher.publish(event("topic", "{\"x-event-type\":\"stored\",\"x-custom\":\"kept\"}"));

    ArgumentCaptor<ProducerRecord<Object, Object>> sent =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(template).send(sent.capture());
    assertThat(sent.getValue().key()).isEqualTo("agg-1");
    assertThat(header(sent.getValue(), "x-idempotency-key")).isEqualTo("svc:outbox:7");
    assertThat(header(sent.getValue(), "x-event-type")).isEqualTo("e.v1"); // relay wins
    assertThat(header(sent.getValue(), "x-custom")).isEqualTo("kept"); // stored survives
    assertThat(header(sent.getValue(), "x-producer")).isEqualTo("svc");
    // REPLACED, not appended: exactly one x-event-type on the wire
    assertThat(sent.getValue().headers().headers("x-event-type")).hasSize(1);
    // the private reference is not a wire header, under any name
    assertThat(sent.getValue().headers().toArray())
        .noneMatch(h -> new String(h.value(), StandardCharsets.UTF_8).contains("subscription-42"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void publish_transactionalTemplate_ridesTheKafkaTransaction() {
    when(template.isTransactional()).thenReturn(true);

    publisher.publish(event("topic", null));

    verify(template).executeInTransaction(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void publish_unacknowledgedSend_throwsForTheRelay() {
    when(template.isTransactional()).thenReturn(false);
    when(template.send(any(ProducerRecord.class)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker gone")));

    OutboxEvent event = event("topic", null);
    assertThatExceptionOfType(KafkaException.class)
        .isThrownBy(() -> publisher.publish(event))
        .withMessageContaining("row 7");
  }

  @Test
  @SuppressWarnings("unchecked")
  void publish_ackTimeout_throwsForTheRelay() {
    OutboxProperties impatient = new OutboxProperties();
    impatient.setSendTimeout(java.time.Duration.ofMillis(50));
    KafkaOutboxPublisher slow =
        new KafkaOutboxPublisher(template, impatient, new ObjectMapper(), "svc");
    when(template.isTransactional()).thenReturn(false);
    when(template.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());

    OutboxEvent event = event("topic", null);
    assertThatExceptionOfType(KafkaException.class)
        .isThrownBy(() -> slow.publish(event))
        .withMessageContaining("Failed to publish");
  }

  @Test
  @SuppressWarnings("unchecked")
  void publish_interrupted_restoresTheFlag_andThrows() {
    when(template.isTransactional()).thenReturn(false);
    when(template.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());

    OutboxEvent event = event("topic", null);
    Thread.currentThread().interrupt();
    try {
      assertThatExceptionOfType(KafkaException.class)
          .isThrownBy(() -> publisher.publish(event))
          .withMessageContaining("Interrupted");
      assertThat(Thread.currentThread().isInterrupted()).isTrue(); // flag restored
    } finally {
      Thread.interrupted(); // clear so the flag never leaks into other tests
    }
  }

  private static String header(ProducerRecord<?, ?> sent, String name) {
    return new String(sent.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
  }
}
