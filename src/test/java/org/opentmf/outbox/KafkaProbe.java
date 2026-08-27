package org.opentmf.outbox;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;

/** IT helper: a fresh-group consumer that drains a topic for a bounded time. */
final class KafkaProbe implements AutoCloseable {

  private final KafkaConsumer<String, String> consumer;

  KafkaProbe(String bootstrapServers, String... topics) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumer = new KafkaConsumer<>(props);
    consumer.subscribe(List.of(topics));
  }

  /** Polls until {@code expected} records arrived or the deadline passes; returns what came. */
  List<ConsumerRecord<String, String>> drain(int expected, Duration atMost) {
    List<ConsumerRecord<String, String>> seen = new ArrayList<>();
    long deadline = System.nanoTime() + atMost.toNanos();
    while (seen.size() < expected && System.nanoTime() < deadline) {
      consumer.poll(Duration.ofMillis(300)).forEach(seen::add);
    }
    return seen;
  }

  static String header(ConsumerRecord<?, ?> r, String name) {
    Header h = r.headers().lastHeader(name);
    return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
  }

  static boolean anyHeaderValueContains(ConsumerRecord<?, ?> r, String needle) {
    for (Header h : r.headers()) {
      if (new String(h.value(), StandardCharsets.UTF_8).contains(needle)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void close() {
    consumer.close();
  }
}
