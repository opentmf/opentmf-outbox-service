package org.opentmf.outbox.internal;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxHeaders;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.OutboxProperties.KafkaOrderingKey;
import org.opentmf.outbox.OutboxPublisher;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/**
 * The default publisher: any non-HTTP destination is a Kafka topic. Message key =
 * {@code aggregateId} (preserves per-aggregate order); relay-stamped headers:
 * {@code x-idempotency-key} = {@code <service>:outbox:<id>}, {@code x-event-type},
 * {@code x-producer} ({@link OutboxHeaders}). Stored headers apply first; relay-stamped ones
 * REPLACE same-named ones. The row's {@code reference} is never sent. {@code traceparent} is
 * stamped by Micrometer's Kafka observation - which the CONSUMER enables with
 * {@code spring.kafka.template.observation-enabled=true} - never home-grown. The record value
 * is the stored JSON STRING: the consumer's value serializer must be string-compatible.
 *
 * <p>When the template is transactional the send runs in a Kafka transaction; otherwise the
 * relay awaits the broker acknowledgement synchronously so a failure is observed before the
 * booking - for as long as the PRODUCER may still deliver the record (1.9.0): its {@code
 * delivery.timeout.ms} plus {@code linger.ms}, at least {@code send-timeout}. The row stays in
 * flight - its key with it - until the broker or the producer answers. Up to 1.8.0 the wait ended
 * at {@code send-timeout} while the record stayed in the producer's buffer: the row backed off,
 * the key's next row passed it from another pod, and the buffered record landed later - one
 * aggregate's events out of order on the topic after a broker outage. The cost: during an outage
 * each Kafka send holds its slot up to the producer's delivery timeout (120 s by default - lower
 * it on the producer to fail faster). {@link #sendTimeout} declares the whole wait, the producer's
 * {@code max.block.ms} included, so the send heartbeat keeps the lease through it.
 *
 * <p>Lane ({@code opentmf.outbox.kafka.lane}, 1.5.0): ORDERED by default - the single relay
 * thread, {@code id} order within a pod, the ordered lease; with several relays rows interleave
 * across pods. CONCURRENT - ordered by {@code kafka.ordering-key} (the {@code aggregateId} by
 * default): one aggregate's rows are never in flight together, on any pod, and go in {@code id}
 * order on the happy path, while different aggregates relay in parallel; the lease is
 * {@code kafka.lease}. The record key stays the raw {@code aggregateId} on either lane, so the
 * partitioning never moves.
 */
class KafkaOutboxPublisher implements OutboxPublisher {

  private final KafkaTemplate<Object, Object> kafkaTemplate;
  private final OutboxProperties properties;
  private final ObjectMapper objectMapper;
  private final String serviceName;

  /** How long the acknowledgement is awaited: the producer's delivery timeout plus its linger. */
  private final Duration acknowledgementWait;

  /** The whole send at most: the producer's {@code max.block.ms} first, then the wait above. */
  private final Duration sendBound;

  KafkaOutboxPublisher(
      KafkaTemplate<Object, Object> kafkaTemplate,
      OutboxProperties properties,
      ObjectMapper objectMapper,
      String serviceName) {
    this.kafkaTemplate = kafkaTemplate;
    this.properties = properties;
    this.objectMapper = objectMapper;
    this.serviceName = serviceName;
    Map<String, Object> producer =
        kafkaTemplate.getProducerFactory() == null
            ? Map.of()
            : kafkaTemplate.getProducerFactory().getConfigurationProperties();
    Duration verdict =
        millis(producer, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)
            .plus(millis(producer, ProducerConfig.LINGER_MS_CONFIG))
            .plus(Duration.ofSeconds(1)); // the producer's own expiry, then the callback
    this.acknowledgementWait = longer(verdict, properties.getSendTimeout());
    this.sendBound =
        longer(
            acknowledgementWait.plus(millis(producer, ProducerConfig.MAX_BLOCK_MS_CONFIG)),
            properties.getSendTimeout());
  }

  /** A producer setting in milliseconds - the consumer's value, else Kafka's default. */
  private static Duration millis(Map<String, Object> producer, String name) {
    Object value = producer.get(name);
    if (value == null) {
      value = ProducerConfig.configDef().defaultValues().get(name);
    }
    return Duration.ofMillis(Long.parseLong(String.valueOf(value)));
  }

  private static Duration longer(Duration a, Duration b) {
    return a.compareTo(b) >= 0 ? a : b;
  }

  /** The whole send: the producer's metadata wait, its delivery timeout and linger (1.9.0). */
  @Override
  public Duration sendTimeout(OutboxEvent event) {
    return sendBound;
  }

  /** The DEFAULT publisher: everything that is not an http(s) URL is a topic name. */
  @Override
  public boolean supports(OutboxEvent event) {
    String destination = event.getDestination();
    return !(destination.startsWith("http://") || destination.startsWith("https://"));
  }

  @Override
  public Lane lane(OutboxEvent event) {
    return properties.getKafka().getLane();
  }

  /** On CONCURRENT: the aggregate (unless {@code ordering-key: NONE}); on ORDERED, none. */
  @Override
  public String orderingKey(OutboxEvent event) {
    OutboxProperties.Kafka kafka = properties.getKafka();
    return kafka.getLane() == Lane.CONCURRENT
            && kafka.getOrderingKey() == KafkaOrderingKey.AGGREGATE_ID
        ? event.getAggregateId()
        : null;
  }

  /** On CONCURRENT: {@code kafka.lease}; on ORDERED, the lane's own ({@code ordered.lease}). */
  @Override
  public Duration lease(OutboxEvent event) {
    return properties.getKafka().getLane() == Lane.CONCURRENT
        ? properties.getKafka().getLease()
        : null;
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
        OutboxHeaders.IDEMPOTENCY_KEY,
        OutboxHeaders.idempotencyKey(serviceName, event.getId()));
    setHeader(producerRecord, OutboxHeaders.EVENT_TYPE, event.getEventType());
    setHeader(producerRecord, OutboxHeaders.PRODUCER, serviceName);
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
      future.get(acknowledgementWait.toMillis(), TimeUnit.MILLISECONDS);
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
