package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.client.RestClient;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerResponse;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/**
 * CONSUMER CONFORMANCE - the dnms-681 profile (H.6): boots over a 681-SHAPED PRE-LIBRARY table
 * (init script + the library include + the consumer's cut-over delta); Kafka rows and hub
 * (HTTP) rows side by side; the hub publisher is the CONSUMER's, ordered ahead, with its own
 * budget (3), its own backoff and DROP on exhaustion; the subscription id rides
 * {@code reference} and never reaches the wire; a listener filtering by destination sees no
 * dropped row; cancel-vs-claim: the claim wins; the /ops wire answers 404 / 409.
 */
@Testcontainers
@SpringBootTest(
    webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = {
      "spring.application.name=dnms-681",
      "spring.datasource.url="
          + "jdbc:tc:postgresql:18.1-alpine3.22:///hub681?TC_INITSCRIPT=db/init-681.sql",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog-681.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
      "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
      "opentmf.outbox.sweep-interval=1s"
    })
class Profile681HubIT {

  @Container static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.1");

  @DynamicPropertySource
  static void kafkaProps(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }

  /** What the hub receiver saw: one entry per delivered POST (path + headers). */
  static final List<Map<String, String>> HUB_SEEN = new CopyOnWriteArrayList<>();

  static final AtomicInteger FAILING_HITS = new AtomicInteger();

  /** Rows the consumer's destination-filtered listener stamped. */
  static final Set<Long> LISTENER_SEEN = ConcurrentHashMap.newKeySet();

  @TestConfiguration
  static class Consumer681 {

    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
      return new TransactionTemplate(txManager);
    }

    /** The hub receiver: /hub/ok answers 200, /hub/down always 503. */
    @Bean
    RouterFunction<ServerResponse> hubReceiver() {
      return RouterFunctions.route()
          .POST(
              "/hub/ok",
              request -> {
                HUB_SEEN.add(request.headers().asHttpHeaders().toSingleValueMap());
                return ServerResponse.ok().build();
              })
          .POST(
              "/hub/down",
              request -> {
                FAILING_HITS.incrementAndGet();
                return ServerResponse.status(503).build();
              })
          .build();
    }

    /**
     * 681's hub sender - the consumer's OWN publisher for hub callbacks, ordered ahead of the
     * library's HTTP default: budget 3, flat 200ms backoff, DROP when exhausted (a hub that keeps
     * failing is the hub's problem - the row must not park and page an operator).
     */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    OutboxPublisher hubSender(ObjectMapper objectMapper) {
      RestClient client = RestClient.create();
      return new OutboxPublisher() {
        @Override
        public boolean supports(OutboxEvent event) {
          return event.getDestination().contains("/hub/");
        }

        @Override
        public void publish(OutboxEvent event) {
          Map<String, String> stored =
              event.getHeaders() == null
                  ? Map.of()
                  : objectMapper.readValue(
                      event.getHeaders(), new TypeReference<Map<String, String>>() {});
          client
              .post()
              .uri(event.getDestination())
              .contentType(MediaType.APPLICATION_JSON)
              .headers(
                  headers -> {
                    stored.forEach(headers::set); // wire headers: forwarded, as the contract says
                    headers.set(
                        OutboxHeaders.IDEMPOTENCY_KEY,
                        OutboxHeaders.idempotencyKey("dnms-681", event.getId()));
                    headers.set(OutboxHeaders.EVENT_TYPE, event.getEventType());
                  })
              .body(event.getPayload())
              .retrieve()
              .toBodilessEntity();
        }

        @Override
        public int maxAttempts(OutboxEvent event) {
          return 3;
        }

        @Override
        public Duration backoff(OutboxEvent event, int attempt) {
          return Duration.ofMillis(200);
        }

        @Override
        public ExhaustionOutcome onExhausted(OutboxEvent event) {
          return ExhaustionOutcome.DROP;
        }
      };
    }

    /** 681's record-state transition: only hub deliveries interest it. */
    @Bean
    OutboxRelayedListener hubDeliveryBookkeeping() {
      return event -> {
        if (event.getDestination().contains("/hub/")) {
          LISTENER_SEEN.add(event.getId());
        }
      };
    }
  }

  @Autowired private OutboxWriter writer;
  @Autowired private OutboxMaintenanceService maintenance;
  @Autowired private TransactionTemplate tx;
  @Autowired private MeterRegistry registry;
  @LocalServerPort private int port;

  private String hub(String path) {
    return "http://localhost:" + port + "/hub/" + path;
  }

  @Test
  void hubProfile_dropsByItsOwnBudget_referenceStaysPrivate_claimBeatsCancel_opsWire() {
    String topic = "hub-681-" + UUID.randomUUID();
    OutboxEvent kafkaRow =
        tx.execute(
            s ->
                writer.append(
                    OutboxAppend.of("subscription", "sub-1", "hub.event.v1", topic, Map.of())
                        .withReference("sub-1")));
    OutboxEvent delivered =
        tx.execute(
            s ->
                writer.append(
                    OutboxAppend.of("subscription", "sub-2", "hub.event.v1", hub("ok"), Map.of())
                        .withHeaders(Map.of("x-schema-version", "3"))
                        .withReference("subscription-secret-2")));
    OutboxEvent dropped =
        tx.execute(
            s ->
                writer.append(
                    OutboxAppend.of("subscription", "sub-3", "hub.event.v1", hub("down"), Map.of())
                        .withReference("subscription-secret-3")));

    // 1) the Kafka row and the healthy hub row relay; the failing hub row is DROPPED after 3
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              assertThat(maintenance.inspect(kafkaRow.getId()).relayedOn()).isNotNull();
              assertThat(maintenance.inspect(delivered.getId()).relayedOn()).isNotNull();
              OutboxRowView view = maintenance.inspect(dropped.getId());
              assertThat(view.relayedOn()).isNotNull(); // left the pending set...
              assertThat(view.attempts()).isEqualTo(3); // ...at the PUBLISHER's budget
              assertThat(view.parked()).isFalse();
              assertThat(view.lastError()).contains("503");
            });
    assertThat(FAILING_HITS.get()).isEqualTo(3); // no 4th attempt ever
    assertThat(
            registry
                .get("opentmf.outbox.dropped")
                .tag("destination", hub("down"))
                .counter()
                .count())
        .isEqualTo(1d);
    assertThat(registry.find("opentmf.outbox.relayed").tag("destination", hub("down")).counter())
        .isNull(); // a drop is not a relay

    // 2) the destination-filtered listener saw the delivered hub row, never the dropped one
    assertThat(delivered.getId()).isIn(LISTENER_SEEN);
    assertThat(dropped.getId()).isNotIn(LISTENER_SEEN);
    assertThat(kafkaRow.getId()).isNotIn(LISTENER_SEEN); // filtered by destination

    // 3) reference is private: the stored header rode the wire, the reference did not
    Map<String, String> wire = HUB_SEEN.get(0);
    assertThat(wire)
        .containsEntry("x-schema-version", "3")
        .containsEntry(OutboxHeaders.IDEMPOTENCY_KEY, "dnms-681:outbox:" + delivered.getId());
    assertThat(wire.toString()).doesNotContain("subscription-secret");
    assertThat(maintenance.inspect(delivered.getId()).reference())
        .isEqualTo("subscription-secret-2");

    // 4) cancel vs claim: the claim won - a relayed row refuses to be cancelled
    assertThatIllegalStateException()
        .isThrownBy(() -> maintenance.cancel(delivered.getId()))
        .withMessageContaining("already relayed");

    // 5) the /ops wire contract: 404 for no such row, 409 for the wrong state
    RestClient ops = RestClient.create("http://localhost:" + port);
    assertThat(status(ops, "/ops/outbox/999999")).isEqualTo(HttpStatusCode.valueOf(404));
    assertThat(status(ops, "/ops/outbox/" + delivered.getId() + "/cancel"))
        .isEqualTo(HttpStatusCode.valueOf(409));
    assertThat(status(ops, "/ops/outbox/" + delivered.getId() + "/unpark"))
        .isEqualTo(HttpStatusCode.valueOf(409));
  }

  private static HttpStatusCode status(RestClient client, String path) {
    RestClient.RequestHeadersSpec<?> request =
        path.endsWith("/cancel") || path.endsWith("/unpark")
            ? client.post().uri(path)
            : client.get().uri(path);
    return request.exchange((req, res) -> res.getStatusCode());
  }
}
