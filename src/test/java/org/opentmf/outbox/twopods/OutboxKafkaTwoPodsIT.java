package org.opentmf.outbox.twopods;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxAppend;
import org.opentmf.outbox.OutboxWriter;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * OUTBOX-KAFKA-PER-KEY-ORDER-1 (1.5.0), on two real pods: two Spring contexts - two relays, two
 * Kafka producers - over ONE database and ONE broker, the library's own Kafka publisher on
 * {@code opentmf.outbox.kafka.lane=CONCURRENT}. Forty aggregates of three lifecycle rows each,
 * one aggregate's rows next to each other in {@code id} (so a claim of five splits most
 * aggregates across two claims - two pods), appended alternately on both pods and all released
 * at ONE instant (both relays face the whole backlog at once), every send slowed by a random
 * 0-15 ms on the producer thread: each aggregate's rows arrive on the topic in append order,
 * whichever pod relayed them, and every row exactly once.
 *
 * <p>The same IT on the 1.4.0 behaviour ({@code -Dit.kafka.lane=ORDERED}) fails - the two pods'
 * ORDERED relays take adjacent claims at once, so a split aggregate's rows race - which is the
 * defect: an adapter's {@code delivered} ahead of its {@code accepted}.
 */
@Testcontainers
class OutboxKafkaTwoPodsIT {

  private static final int AGGREGATES = 40;
  private static final String[] PHASES = {"accepted", "dispatched", "delivered"};

  @Container
  static final PostgreSQLContainer postgres =
      new PostgreSQLContainer("postgres:18.1-alpine3.22");

  @Container static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.1");

  /** A consumer-shaped app of its own package: scans nothing of the other ITs. */
  @SpringBootApplication
  static class Pod {}

  /** The broker's latency, on the producing (relay) thread: widens every cross-pod race. */
  public static class SlowSend implements ProducerInterceptor<Object, Object> {

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> producerRecord) {
      try { // a timed wait on a latch nobody opens
        assertThat(new CountDownLatch(1).await(ThreadLocalRandom.current().nextLong(16), MILLISECONDS))
            .isFalse();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
      return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
      // nothing to observe
    }

    @Override
    public void close() {
      // nothing held
    }

    @Override
    public void configure(Map<String, ?> configs) {
      // no settings
    }
  }

  private static ConfigurableApplicationContext podA;
  private static ConfigurableApplicationContext podB;

  private static ConfigurableApplicationContext pod(String name) {
    return new SpringApplicationBuilder(Pod.class)
        .properties(
            "spring.application.name=two-pods-it",
            "spring.main.banner-mode=off",
            "server.port=0",
            "spring.datasource.url=" + postgres.getJdbcUrl(),
            "spring.datasource.username=" + postgres.getUsername(),
            "spring.datasource.password=" + postgres.getPassword(),
            "spring.liquibase.change-log=classpath:db/test-changelog.xml",
            "spring.jpa.hibernate.ddl-auto=validate",
            "spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
            "spring.kafka.producer.key-serializer="
                + "org.apache.kafka.common.serialization.StringSerializer",
            "spring.kafka.producer.value-serializer="
                + "org.apache.kafka.common.serialization.StringSerializer",
            "spring.kafka.producer.properties.interceptor.classes=" + SlowSend.class.getName(),
            "spring.kafka.producer.client-id=" + name,
            // -Dit.kafka.lane=ORDERED reproduces the 1.4.0 defect (the IT then fails)
            "opentmf.outbox.kafka.lane=" + System.getProperty("it.kafka.lane", "CONCURRENT"),
            "opentmf.outbox.sweep-interval=200ms",
            "opentmf.outbox.batch-size=5")
        .run();
  }

  @BeforeAll
  static void twoPods() {
    podA = pod("pod-a"); // migrates the schema
    podB = pod("pod-b");
  }

  @AfterAll
  static void stop() {
    if (podB != null) {
      podB.close();
    }
    if (podA != null) {
      podA.close();
    }
  }

  @Test
  void everyAggregatesRows_arriveInOrder_exactlyOnce_whicheverPodRelaysThem() {
    String topic = "lifecycle-" + UUID.randomUUID();
    List<String> aggregates = new ArrayList<>();
    for (int i = 0; i < AGGREGATES; i++) {
      aggregates.add("msg-" + i + "-" + UUID.randomUUID());
    }

    try (KafkaConsumer<String, String> consumer = consumer(topic)) {
      // one business transaction per row, alternately on BOTH pods, every row held until ONE
      // instant: no relay drains the rows as they come, both find the whole backlog together
      OffsetDateTime releaseAt = OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(10);
      int n = 0;
      for (String aggregate : aggregates) {
        for (String phase : PHASES) {
          append(n++ % 2 == 0 ? podA : podB, aggregate, phase, topic, releaseAt);
        }
      }

      List<ConsumerRecord<String, String>> records =
          drain(consumer, AGGREGATES * PHASES.length, Duration.ofSeconds(60));

      Map<String, Integer> perIdempotencyKey = new HashMap<>();
      Map<String, List<String>> phasesPerAggregate = new LinkedHashMap<>();
      for (ConsumerRecord<String, String> r : records) {
        perIdempotencyKey.merge(header(r, "x-idempotency-key"), 1, Integer::sum);
        phasesPerAggregate
            .computeIfAbsent(r.key(), k -> new ArrayList<>())
            .add(header(r, "x-event-type"));
      }
      assertThat(records).hasSize(AGGREGATES * PHASES.length);
      assertThat(perIdempotencyKey).hasSize(AGGREGATES * PHASES.length).allSatisfy(
          (key, count) -> assertThat(count).as(key).isEqualTo(1)); // exactly once
      assertThat(phasesPerAggregate.keySet()).containsExactlyInAnyOrderElementsOf(aggregates);
      phasesPerAggregate.forEach(
          (aggregate, phases) ->
              assertThat(phases)
                  .as("phases of %s, in topic order", aggregate)
                  .containsExactly(
                      "email.accepted.v1", "email.dispatched.v1", "email.delivered.v1"));
    }
  }

  private static void append(
      ConfigurableApplicationContext pod,
      String aggregate,
      String phase,
      String topic,
      OffsetDateTime releaseAt) {
    OutboxWriter writer = pod.getBean(OutboxWriter.class);
    OutboxAppend row =
        OutboxAppend.of("message", aggregate, "email." + phase + ".v1", topic, Map.of("p", phase))
            .withReleaseAt(releaseAt);
    new TransactionTemplate(pod.getBean(PlatformTransactionManager.class))
        .executeWithoutResult(s -> writer.append(row));
  }

  private static KafkaConsumer<String, String> consumer(String topic) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(List.of(topic));
    return consumer;
  }

  /** Polls until {@code expected} records arrived and a quiet second more (no duplicates). */
  private static List<ConsumerRecord<String, String>> drain(
      KafkaConsumer<String, String> consumer, int expected, Duration atMost) {
    List<ConsumerRecord<String, String>> seen = new ArrayList<>();
    long deadline = System.nanoTime() + atMost.toNanos();
    while (seen.size() < expected && System.nanoTime() < deadline) {
      consumer.poll(Duration.ofMillis(300)).forEach(seen::add);
    }
    long quiet = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (System.nanoTime() < quiet) { // a duplicate would arrive here
      consumer.poll(Duration.ofMillis(300)).forEach(seen::add);
    }
    return seen;
  }

  private static String header(ConsumerRecord<?, ?> r, String name) {
    Header h = r.headers().lastHeader(name);
    return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
  }
}
