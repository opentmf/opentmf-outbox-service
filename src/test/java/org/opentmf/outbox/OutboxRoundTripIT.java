package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.data.domain.PageRequest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The library's contract end to end on the REAL engines (no H2):
 * a business transaction appends through the writer; the relay delivers to Kafka with the
 * headers; the /ops surface serves the TMF630 list + inspect + prune + cancel over the same
 * rows; the claim predicate honours the hold and the cancellation on the REAL query. Postgres
 * rides the {@code jdbc:tc:} URL so the REAL library changelog (both changesets) runs, and
 * {@code ddl-auto=validate} proves the entity matches it.
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

  /** The row whose FIRST claim transaction is rolled back after the publish (crash window). */
  static final AtomicLong CRASH_AFTER_PUBLISH_OF = new AtomicLong(-1);

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

    /**
     * Models the crash window: the effect is delivered, then the claim transaction does NOT
     * commit (rollback-only, once). Nothing is booked - not even attempts++ - so the row comes
     * back exactly as it was and is published again.
     */
    @Bean
    OutboxRelayedListener crashWindowListener() {
      return event -> {
        if (CRASH_AFTER_PUBLISH_OF.compareAndSet(event.getId(), -1)) {
          TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
        }
      };
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
        .andExpect(jsonPath("$").isArray())
        .andExpect(jsonPath("$").isEmpty());

    // the state legs on the PATH, with the toolkit predicate still applying on top - and the
    // literal "state" segment routes past /ops/outbox/{id} (a two-segment path, no clash)
    mockMvc
        .perform(get("/ops/outbox/state/relayed").param("eventType", "it.event.v1"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[?(@.id == %d)]".formatted(appended.getId())).isNotEmpty());
    mockMvc
        .perform(get("/ops/outbox/state/pending").param("eventType", "it.event.v1"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[?(@.id == %d)]".formatted(appended.getId())).isEmpty());
    mockMvc.perform(get("/ops/outbox/state/parked")).andExpect(status().isOk());
    mockMvc.perform(get("/ops/outbox/state/dead-lettered")).andExpect(status().isBadRequest());
    mockMvc
        .perform(get("/ops/outbox/{id}", appended.getId()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.id").value(appended.getId()));

    // the toolkit's OWN strictness stands untouched: an unknown filter field is a 400
    mockMvc
        .perform(get("/ops/outbox").param("noSuchField", "x"))
        .andExpect(status().is4xxClientError());

    mockMvc
        .perform(post("/ops/outbox/maintenance/prune"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.outboxRowsPruned").isNumber());
  }

  /**
   * CR §23 crash window (H.4): publish succeeds, the claim transaction rolls back before
   * commit - the row is redelivered on the next pass with the SAME idempotency key, and the
   * outcome is at-least-once: two records on the wire, one row relayed, no attempt booked.
   */
  @Test
  void aRollbackAfterPublish_redeliversWithTheSameIdempotencyKey() {
    String topic = "it-crash-" + UUID.randomUUID();
    try (KafkaProbe probe = new KafkaProbe(kafka.getBootstrapServers(), topic)) {
      OutboxEvent row =
          tx.execute(
              status -> {
                OutboxEvent e =
                    writer.append("it-aggregate", "a-crash", "it.event.v1", topic, Map.of());
                CRASH_AFTER_PUBLISH_OF.set(e.getId()); // arm before the after-commit nudge
                return e;
              });

      var records = probe.drain(2, Duration.ofSeconds(30));

      assertThat(records).hasSize(2); // delivered twice...
      assertThat(records)
          .allSatisfy(
              r ->
                  assertThat(KafkaProbe.header(r, OutboxHeaders.IDEMPOTENCY_KEY))
                      .isEqualTo("outbox-it:outbox:" + row.getId())); // ...same key: dedupable
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> assertThat(maintenance.inspect(row.getId()).relayedOn()).isNotNull());
      assertThat(maintenance.inspect(row.getId()).attempts()).isZero(); // a rollback, not a failure
    }
  }

  /**
   * The /ops wire contract (1.2.0-E): 404 for no such row, 409 for a row not in the state the
   * action needs; the toolkit's own 400 for an unknown filter field stands untouched.
   */
  @Test
  void theOpsWire_answers404ForUnknownRows_and409ForTheWrongState() throws Exception {
    OutboxEvent relayed =
        tx.execute(
            status ->
                writer.append("it-aggregate", "a-2", "it.event.v1", "it-ops-wire", Map.of()));
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(maintenance.inspect(relayed.getId()).relayedOn()).isNotNull());

    mockMvc.perform(get("/ops/outbox/{id}", 987654321L)).andExpect(status().isNotFound());
    mockMvc.perform(post("/ops/outbox/{id}/unpark", 987654321L)).andExpect(status().isNotFound());
    mockMvc
        .perform(post("/ops/outbox/{id}/cancel", relayed.getId()))
        .andExpect(status().isConflict());
    mockMvc
        .perform(post("/ops/outbox/{id}/unpark", relayed.getId()))
        .andExpect(status().isConflict());
  }

  /**
   * The two claim-predicate properties on the real query: a HELD row is not claimed before
   * its {@code release_at} and IS claimed after; a CANCELLED row is never claimed - and the
   * cancel guard refuses a relayed row.
   */
  @Test
  void aHeldRow_relaysOnlyAfterItsHold_andACancelledRow_never() throws Exception {
    String topic = "it-hold-" + UUID.randomUUID();
    OffsetDateTime hold = OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(4);
    OutboxEvent held =
        tx.execute(
            status ->
                writer.append(
                    OutboxAppend.of("it-aggregate", "a-held", "it.event.v1", topic, Map.of())
                        .withReleaseAt(hold)));
    // the row to cancel carries a SHORT hold (the scheduled-send-then-withdraw use case): an
    // unheld row races the after-commit nudge, and the cancel guard would rightly refuse it
    OffsetDateTime shortHold = OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(2);
    OutboxEvent toCancel =
        tx.execute(
            status ->
                writer.append(
                    OutboxAppend.of("it-aggregate", "a-cancel", "it.event.v1", topic, Map.of())
                        .withReleaseAt(shortHold)));
    OutboxEvent plain =
        tx.execute(
            status -> writer.append("it-aggregate", "a-plain", "it.event.v1", topic, Map.of()));

    // cancel over the wire while the row is held
    mockMvc
        .perform(post("/ops/outbox/{id}/cancel", toCancel.getId()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.action").value("cancelled"));

    // the plain row relays; at that moment the held row is still pending (hold not passed)
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(maintenance.inspect(plain.getId()).relayedOn()).isNotNull());
    OutboxRowView heldView = maintenance.inspect(held.getId());
    assertThat(heldView.releaseAt()).isNotNull();
    if (OffsetDateTime.now(ZoneOffset.UTC).isBefore(hold)) {
      assertThat(heldView.relayedOn()).isNull();
    }

    // the hold passes: the SAME row relays with the hold untouched
    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(
            () -> assertThat(maintenance.inspect(held.getId()).relayedOn()).isNotNull());
    assertThat(maintenance.inspect(held.getId()).relayedOn()).isAfterOrEqualTo(hold);
    assertThat(maintenance.inspect(held.getId()).releaseAt()).isEqualTo(heldView.releaseAt());

    // the cancelled row's own hold has long passed (the 4s hold above was awaited) - and it
    // still never relayed: cancellation, not the hold, kept it out of the claim
    OutboxRowView cancelledView = maintenance.inspect(toCancel.getId());
    assertThat(cancelledView.relayedOn()).isNull();
    assertThat(cancelledView.cancelledOn()).isNotNull();
    assertThat(
            maintenance
                .list(null, OutboxStateFilter.CANCELLED, PageRequest.of(0, 50))
                .map(OutboxRowView::id))
        .contains(toCancel.getId());
    assertThat(
            maintenance
                .list(null, OutboxStateFilter.PENDING, PageRequest.of(0, 50))
                .map(OutboxRowView::id))
        .doesNotContain(toCancel.getId(), held.getId(), plain.getId());

    // the guard: a relayed effect has left - it cannot be cancelled
    assertThatIllegalStateException()
        .isThrownBy(() -> maintenance.cancel(plain.getId()))
        .withMessageContaining("already relayed");
  }
}
