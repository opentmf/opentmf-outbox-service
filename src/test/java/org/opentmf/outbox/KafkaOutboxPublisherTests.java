package org.opentmf.outbox;

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
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;

/** S14 headers stamped, key = aggregateId, ack awaited, failures unwind to the relay. */
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

    ArgumentCaptor<ProducerRecord<Object, Object>> record =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(template).send(record.capture());
    assertThat(record.getValue().key()).isEqualTo("agg-1");
    assertThat(header(record.getValue(), "x-idempotency-key")).isEqualTo("svc:outbox:7");
    assertThat(header(record.getValue(), "x-event-type")).isEqualTo("e.v1"); // relay wins
    assertThat(header(record.getValue(), "x-custom")).isEqualTo("kept"); // stored survives
    assertThat(header(record.getValue(), "x-producer")).isEqualTo("svc");
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

    assertThatExceptionOfType(KafkaException.class)
        .isThrownBy(() -> publisher.publish(event("topic", null)))
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

    assertThatExceptionOfType(KafkaException.class)
        .isThrownBy(() -> slow.publish(event("topic", null)))
        .withMessageContaining("Failed to publish");
  }

  @Test
  @SuppressWarnings("unchecked")
  void publish_interrupted_restoresTheFlag_andThrows() {
    when(template.isTransactional()).thenReturn(false);
    when(template.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());

    Thread.currentThread().interrupt();
    try {
      assertThatExceptionOfType(KafkaException.class)
          .isThrownBy(() -> publisher.publish(event("topic", null)))
          .withMessageContaining("Interrupted");
      assertThat(Thread.currentThread().isInterrupted()).isTrue(); // flag restored
    } finally {
      Thread.interrupted(); // clear so the flag never leaks into other tests
    }
  }

  private static String header(ProducerRecord<?, ?> record, String name) {
    return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
  }
}
