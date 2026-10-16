package org.opentmf.outboxinvariants;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opentmf.outbox.OutboxHeaders;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.opentmf.outboxinvariants.Receiver.Delivery;
import org.opentmf.outboxinvariants.Receiver.Outcome;

/**
 * Reads what the relay put on a topic and records each record as a delivery: the row id from the
 * idempotency key, the record key as the ordering key. A record carries no send start, so start =
 * end = its position (the partition offset) - enough for per-key order, not for timing.
 */
final class KafkaTap implements AutoCloseable {

  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Thread poller;
  private final Lane lane;

  KafkaTap(String topic, Lane lane) {
    this.lane = lane;
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Infra.kafka().getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "tap-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(List.of(topic));
    poller =
        new Thread(
            () -> {
              try (consumer) {
                while (running.get()) {
                  for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
                    record(r);
                  }
                }
              }
            },
            "kafka-tap-" + topic);
    poller.setDaemon(true);
    poller.start();
  }

  private void record(ConsumerRecord<String, String> r) {
    Header key = r.headers().lastHeader(OutboxHeaders.IDEMPOTENCY_KEY);
    if (key == null) {
      return;
    }
    String text = new String(key.value(), StandardCharsets.UTF_8);
    long rowId = Long.parseLong(text.substring(text.lastIndexOf(':') + 1));
    Instant position = Instant.EPOCH.plusNanos(r.partition() * 1_000_000_000_000L + r.offset());
    Receiver.record(
        new Delivery(
            rowId,
            r.topic(),
            lane == Lane.CONCURRENT ? r.key() : null,
            lane,
            "kafka",
            1,
            position,
            position,
            Outcome.OK,
            false));
  }

  @Override
  public void close() {
    running.set(false);
    try {
      poller.join(5000);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }
}
