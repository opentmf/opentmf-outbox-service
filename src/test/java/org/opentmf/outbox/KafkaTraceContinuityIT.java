package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.observation.ObservationRegistry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.trace.data.SpanData;
import jakarta.persistence.EntityManagerFactory;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.fixture.TestTracing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * OUTBOX-SEND-OBSERVATION-PARENT-1, end to end: a consumer with Spring Kafka's template observation
 * on ({@code spring.kafka.template.observation-enabled=true}) and a tracer behind its {@code
 * ObservationRegistry} appends a row whose stored headers carry the request's W3C {@code
 * traceparent}, and one without. The record read from the broker must continue the stored trace -
 * its trace id, not the stored span - and the row without one must start a trace of its own, as in
 * 1.11.0. Up to 1.11.0 the first record carried a new root as well. On both lanes and with a plain
 * and a transactional producer: the relay span must be current on the thread that sends, whichever
 * that is.
 *
 * <p>One context per combination: the subclasses set the lane and the producer kind.
 *
 * <p>The tracer is wired by {@link TestTracing}, not by Boot's tracing auto-configuration, which is
 * not on the test classpath: no other test context traces anything.
 */
@Testcontainers
@SpringBootTest(
    properties = {
      "spring.application.name=outbox-trace-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///tracecontinuity",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
      "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
      "spring.kafka.template.observation-enabled=true",
      "opentmf.outbox.sweep-interval=1s"
    })
@DirtiesContext
@Import(KafkaTraceContinuityIT.Tracing.class)
abstract class KafkaTraceContinuityIT {

  static final String TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";
  static final String STORED_SPAN = "00f067aa0ba902b7";
  static final String STORED = "00-" + TRACE + "-" + STORED_SPAN + "-01";

  @Container static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.1");

  @DynamicPropertySource
  static void kafkaProps(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class Tracing {

    @Bean
    TestTracing testTracing() {
      return new TestTracing();
    }

    /** The application's registry, its tracing handlers on - Boot's own then backs off. */
    @Bean
    ObservationRegistry observationRegistry(TestTracing tracing) {
      return tracing.registry();
    }

    /**
     * With a {@code transaction-id-prefix} Boot also registers a {@code KafkaTransactionManager}:
     * the JPA one is declared primary, as a consumer does - the append's database transaction.
     */
    @Bean
    @Primary
    PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
      return new JpaTransactionManager(entityManagerFactory);
    }
  }

  @Autowired private OutboxWriter writer;
  @Autowired private PlatformTransactionManager txManager;
  @Autowired private TestTracing tracing;

  @Test
  void aStoredTraceparent_continues_andARowWithoutOne_startsItsOwnTrace() {
    String topic = "trace-" + UUID.randomUUID();
    TransactionTemplate tx = new TransactionTemplate(txManager);
    tx.executeWithoutResult(
        s -> {
          writer.append(
              "thing",
              "agg-traced",
              "thing.created.v1",
              topic,
              Map.of(),
              Map.of("traceparent", STORED));
          writer.append("thing", "agg-untraced", "thing.created.v1", topic, Map.of());
        });

    List<ConsumerRecord<String, String>> records;
    try (KafkaProbe probe = new KafkaProbe(kafka.getBootstrapServers(), topic)) {
      records = probe.drain(2, Duration.ofSeconds(30));
    }
    assertThat(records).as("both rows on the topic").hasSize(2);
    String traced = traceparentOf(records, "agg-traced");
    String untraced = traceparentOf(records, "agg-untraced");

    assertThat(TestTracing.traceId(traced)).as("the stored trace continues").isEqualTo(TRACE);
    assertThat(TestTracing.spanId(traced))
        .as("not the stored span itself")
        .isNotEqualTo(STORED_SPAN);
    assertThat(TestTracing.traceId(untraced))
        .as("a trace of its own, as before")
        .isNotEqualTo(TRACE);

    SpanData relay = tracing.span("outbox relay");
    assertThat(relay.getParentSpanId()).as("relay span <- stored span").isEqualTo(STORED_SPAN);
    List<SpanData> producers =
        tracing.spans().stream()
            .filter(span -> span.getKind() == SpanKind.PRODUCER)
            .filter(span -> span.getTraceId().equals(TRACE))
            .toList();
    assertThat(producers).as("one producer span in the stored trace").hasSize(1);
    assertThat(producers.get(0).getParentSpanId()).isEqualTo(relay.getSpanId());
    assertThat(producers.get(0).getSpanId())
        .as("the header that left")
        .isEqualTo(TestTracing.spanId(traced));
  }

  private static String traceparentOf(List<ConsumerRecord<String, String>> records, String key) {
    ConsumerRecord<String, String> found =
        records.stream().filter(r -> key.equals(r.key())).findFirst().orElseThrow();
    String traceparent = KafkaProbe.header(found, "traceparent");
    assertThat(traceparent).as("%s carries a traceparent", key).isNotNull();
    return traceparent;
  }
}
