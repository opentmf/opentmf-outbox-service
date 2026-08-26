package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The library's contract end to end on the REAL engines (no H2):
 * a business transaction appends through the writer; the relay delivers to Kafka with the
 * headers; the /ops trio serves the TMF630 list + inspect + prune over the same rows. Postgres
 * rides the {@code jdbc:tc:} URL so the REAL library changelog runs.
 */
@Testcontainers
@AutoConfigureMockMvc
@SpringBootTest(
    properties = {
      "spring.application.name=outbox-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///outbox",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
      "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
      "opentmf.outbox.sweep-interval=1s"
    })
class OutboxRoundTripIT {

  @Container
  static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.1");

  @DynamicPropertySource
  static void kafkaProps(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }

  /** Rows the post-relay seam saw — proves the auto-config collects listener beans. */
  static final Set<Long> RELAYED_SEEN = ConcurrentHashMap.newKeySet();

  @TestConfiguration
  static class TxTemplateConfig {
    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
      return new TransactionTemplate(txManager);
    }

    @Bean
    OutboxRelayedListener recordingRelayedListener() {
      return event -> RELAYED_SEEN.add(event.getId());
    }
  }

  @Autowired private OutboxWriter writer;
  @Autowired private OutboxMaintenanceService maintenance;
  @Autowired private TransactionTemplate tx;
  @Autowired private MockMvc mockMvc;

  private KafkaConsumer<String, String> consumer(String topic) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
    kafkaConsumer.subscribe(List.of(topic));
    return kafkaConsumer;
  }

  @Test
  void appendedInATransaction_relaysToKafka_withTheIdempotencyHeaders() {
    String topic = "it-roundtrip-" + UUID.randomUUID();
    String aggregateId = "agg-" + UUID.randomUUID();

    OutboxEvent appended =
        tx.execute(
            status ->
                writer.append(
                    "it-aggregate", aggregateId, "it.event.v1", topic,
                    Map.of("hello", "outbox")));
    assertThat(appended.getId()).isNotNull();

    try (KafkaConsumer<String, String> kafkaConsumer = consumer(topic)) {
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                boolean seen = false;
                for (ConsumerRecord<String, String> r :
                    kafkaConsumer.poll(Duration.ofMillis(500))) {
                  assertThat(r.key()).isEqualTo(aggregateId);
                  assertThat(r.value()).contains("outbox");
                  assertThat(new String(r.headers().lastHeader("x-idempotency-key").value()))
                      .isEqualTo("outbox-it:outbox:" + appended.getId());
                  assertThat(new String(r.headers().lastHeader("x-event-type").value()))
                      .isEqualTo("it.event.v1");
                  seen = true;
                }
                assertThat(seen).isTrue();
              });
    }

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(maintenance.inspect(appended.getId()).relayedOn()).isNotNull());

    // the post-relay seam fired for this row (auto-config collected the listener bean)
    assertThat(appended.getId()).isIn(RELAYED_SEEN);
  }

  @Test
  void theOpsTrio_listsWithTheStateFilter_inspects_andPrunes() throws Exception {
    String topic = "it-ops-" + UUID.randomUUID();
    OutboxEvent appended =
        tx.execute(status -> writer.append("it-aggregate", "a-1", "it.event.v1", topic, Map.of()));

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(maintenance.inspect(appended.getId()).relayedOn()).isNotNull());

    mockMvc
        .perform(get("/ops/outbox").param("eventType", "it.event.v1"))
        .andExpect(status().isOk())
        .andExpect(
            jsonPath("$[?(@.id == %d)].payload".formatted(appended.getId()))
                .value(org.hamcrest.Matchers.contains((Object) null)));

    mockMvc
        .perform(get("/ops/outbox/{id}", appended.getId()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.payload").isNotEmpty());

    // the derived-state sub-resource: nothing parks in a healthy round-trip — and the wire
    // shape is the toolkit's BARE ARRAY, never raw PageImpl JSON (one surface, one shape)
    mockMvc
        .perform(get("/ops/outbox/parked"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$").isArray());

    // the toolkit's OWN strictness stands untouched: an unknown filter field is a 400
    mockMvc
        .perform(get("/ops/outbox").param("noSuchField", "x"))
        .andExpect(status().is4xxClientError());

    mockMvc
        .perform(post("/ops/outbox/maintenance/prune"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.outboxRowsPruned").isNumber());
  }
}
