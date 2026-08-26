package org.opentmf.outbox;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/**
 * The default publisher: any non-HTTP destination is a Kafka topic. Message key =
 * {@code aggregateId} (preserves per-aggregate order); relay-stamped S14 headers:
 * {@code x-idempotency-key} = {@code <service>:outbox:<id>}, {@code x-event-type},
 * {@code x-producer}. Stored headers apply first; relay-stamped ones win. {@code traceparent}
 * is stamped by Micrometer's Kafka observation, never home-grown.
 *
 * <p>When the template is transactional the send runs in a Kafka transaction; otherwise the
 * relay awaits the broker acknowledgement synchronously so a failure is observed inside the
 * claim transaction.
 */
class KafkaOutboxPublisher implements OutboxPublisher {

  static final String HEADER_IDEMPOTENCY_KEY = "x-idempotency-key";
  static final String HEADER_EVENT_TYPE = "x-event-type";
  static final String HEADER_PRODUCER = "x-producer";

  private final KafkaTemplate<Object, Object> kafkaTemplate;
  private final OutboxProperties properties;
  private final ObjectMapper objectMapper;
  private final String serviceName;

  KafkaOutboxPublisher(
      KafkaTemplate<Object, Object> kafkaTemplate,
      OutboxProperties properties,
      ObjectMapper objectMapper,
      String serviceName) {
    this.kafkaTemplate = kafkaTemplate;
    this.properties = properties;
    this.objectMapper = objectMapper;
    this.serviceName = serviceName;
  }

  /** The DEFAULT publisher: everything that is not an http(s) URL is a topic name. */
  @Override
  public boolean supports(OutboxEvent event) {
    String destination = event.getDestination();
    return !(destination.startsWith("http://") || destination.startsWith("https://"));
  }

  @Override
  public void publish(OutboxEvent event) {
    ProducerRecord<Object, Object> producerRecord = toProducerRecord(event);
    if (kafkaTemplate.isTransactional()) {
      kafkaTemplate.executeInTransaction(operations -> operations.send(producerRecord));
    } else {
      awaitAcknowledgement(kafkaTemplate.send(producerRecord), event);
    }
  }

  private ProducerRecord<Object, Object> toProducerRecord(OutboxEvent event) {
    ProducerRecord<Object, Object> producerRecord =
        new ProducerRecord<>(event.getDestination(), event.getAggregateId(), event.getPayload());
    storedHeaders(event).forEach((name, value) -> setHeader(producerRecord, name, value));
    setHeader(
        producerRecord,
        HEADER_IDEMPOTENCY_KEY,
        "%s:outbox:%d".formatted(serviceName, event.getId()));
    setHeader(producerRecord, HEADER_EVENT_TYPE, event.getEventType());
    setHeader(producerRecord, HEADER_PRODUCER, serviceName);
    return producerRecord;
  }

  private Map<String, String> storedHeaders(OutboxEvent event) {
    if (event.getHeaders() == null) {
      return Map.of();
    }
    return objectMapper.readValue(event.getHeaders(), new TypeReference<Map<String, String>>() {});
  }

  private static void setHeader(ProducerRecord<?, ?> producerRecord, String name, String value) {
    producerRecord.headers().remove(name);
    producerRecord.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
  }

  private void awaitAcknowledgement(
      CompletableFuture<SendResult<Object, Object>> future, OutboxEvent event) {
    try {
      future.get(properties.getSendTimeout().toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new KafkaException(
          "Interrupted while publishing outbox row %d".formatted(event.getId()), ex);
    } catch (ExecutionException | TimeoutException ex) {
      throw new KafkaException(
          "Failed to publish outbox row %d to %s"
              .formatted(event.getId(), event.getDestination()),
          ex);
    }
  }
}
