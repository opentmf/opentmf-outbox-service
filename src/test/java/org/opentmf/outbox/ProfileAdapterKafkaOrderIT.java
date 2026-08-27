package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * CONSUMER CONFORMANCE - the email / inbox adapter profile (H.6): the lifecycle facts of one
 * message ({@code accepted < dispatched < delivered}) appended in three business transactions
 * arrive on Kafka IN THAT ORDER under the SAME key (the aggregate id); the origin-lane
 * destinations (one topic per lane) each get their rows; the frozen {@code x-schema-version}
 * header survives; {@code x-idempotency-key} carries the {@code <app>:outbox:} prefix and
 * {@code x-producer} the app name; the value is the stored JSON string.
 */
@Testcontainers
@SpringBootTest(
    properties = {
      "spring.application.name=dnms-email-adapter",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///adapter",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
      "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
      "opentmf.outbox.sweep-interval=1s"
    })
class ProfileAdapterKafkaOrderIT {

  @Container static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.1");

  @DynamicPropertySource
  static void kafkaProps(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }

  @TestConfiguration
  static class ConsumerAdapter {
    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
      return new TransactionTemplate(txManager);
    }
  }

  @Autowired private OutboxWriter writer;
  @Autowired private TransactionTemplate tx;

  private OutboxEvent lifecycle(String lane, String messageId, String phase) {
    return tx.execute(
        s ->
            writer.append(
                "message", messageId, "email." + phase + ".v1", lane,
                Map.of("phase", phase), Map.of("x-schema-version", "1")));
  }

  @Test
  void lifecyclePhases_arriveInOrderPerKey_onTheirLane_withTheFrozenAndRelayHeaders() {
    String interactive = "email.lifecycle.interactive-" + UUID.randomUUID();
    String bulk = "email.lifecycle.bulk-" + UUID.randomUUID();
    String messageId = "msg-" + UUID.randomUUID();

    try (KafkaProbe probe = new KafkaProbe(kafka.getBootstrapServers(), interactive, bulk)) {
      lifecycle(interactive, messageId, "accepted");
      lifecycle(bulk, "msg-bulk-1", "accepted");
      lifecycle(interactive, messageId, "dispatched");
      lifecycle(interactive, messageId, "delivered");

      List<ConsumerRecord<String, String>> records = probe.drain(4, Duration.ofSeconds(30));

      assertThat(records).hasSize(4);
      // per key, in append order - the single relay thread publishes ascending by id
      assertThat(
              records.stream()
                  .filter(r -> r.key().equals(messageId))
                  .map(r -> KafkaProbe.header(r, OutboxHeaders.EVENT_TYPE))
                  .toList())
          .containsExactly("email.accepted.v1", "email.dispatched.v1", "email.delivered.v1");
      // the origin lanes: each topic got exactly its rows
      assertThat(records.stream().filter(r -> r.topic().equals(bulk)).toList()).hasSize(1);
      assertThat(records.stream().filter(r -> r.topic().equals(interactive)).toList()).hasSize(3);
      assertThat(records)
          .allSatisfy(
              r -> {
                assertThat(KafkaProbe.header(r, "x-schema-version")).isEqualTo("1"); // frozen
                assertThat(KafkaProbe.header(r, OutboxHeaders.IDEMPOTENCY_KEY))
                    .startsWith("dnms-email-adapter:outbox:");
                assertThat(KafkaProbe.header(r, OutboxHeaders.PRODUCER))
                    .isEqualTo("dnms-email-adapter");
                assertThat(r.value()).startsWith("{").contains("\"phase\""); // the JSON string
              });
    }
  }
}
