package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * OUTBOX-HTTP-LANE-1 (1.3.0): HTTP rows leave the ordered relay. The library's HTTP publisher is
 * CONCURRENT (keyed per receiver) and the Kafka publisher ORDERED, against the real engines -
 * PostgreSQL, Kafka, and the service's own web server as the stub subscriber. The relay sweep is
 * parked (1h): every pass here is a nudge, so the connection-pool readings see only the relay's
 * own work. {@link OutboxHttpLaneVirtualIT} reruns all of it on virtual threads (JDK 21+).
 */
@Testcontainers
@SpringBootTest(
    webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = {
      "spring.application.name=outbox-lane-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///lane",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
      "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
      "opentmf.outbox.sweep-interval=1h"
    })
class OutboxHttpLaneIT {

  @Container static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.1");

  @DynamicPropertySource
  static void kafkaProps(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }

  /** Counted down once the slow subscriber has the request in hand. */
  static final CountDownLatch SLOW_RECEIVED = new CountDownLatch(1);

  /** Arrival order at the keyed receivers: "<path>:<idempotency key>". */
  static final List<String> ARRIVALS = new CopyOnWriteArrayList<>();

  /** Requests the /stub/keyed receiver is serving right now, and the most it ever served. */
  static final AtomicInteger KEYED_NOW = new AtomicInteger();
  static final AtomicInteger KEYED_MAX = new AtomicInteger();

  /** The burst: requests held right now, the most ever held, and the gate that frees them. */
  static final AtomicInteger HELD_NOW = new AtomicInteger();
  static final AtomicInteger HELD_MAX = new AtomicInteger();
  static final CountDownLatch BURST_GATE = new CountDownLatch(1);

  /** Per HTTP row booked: was a transaction active, and was the booking thread virtual. */
  static final List<Boolean> BOOKED_IN_TX = new CopyOnWriteArrayList<>();
  static final List<Boolean> BOOKED_ON_VIRTUAL = new CopyOnWriteArrayList<>();

  @TestConfiguration
  static class Stub {

    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
      return new TransactionTemplate(txManager);
    }

    @Bean
    RouterFunction<ServerResponse> stubSubscriber() {
      return RouterFunctions.route()
          // the slow subscriber of the defect: answers 200 after 30 s
          .POST(
              "/stub/slow",
              request -> {
                SLOW_RECEIVED.countDown();
                latency(Duration.ofSeconds(30));
                return ServerResponse.ok().build();
              })
          // one receiver, slow-ish: 1.5 s per request, tracking overlap
          .POST(
              "/stub/keyed",
              request -> {
                int now = KEYED_NOW.incrementAndGet();
                KEYED_MAX.accumulateAndGet(now, Math::max);
                ARRIVALS.add("keyed:" + key(request));
                latency(Duration.ofMillis(1500));
                KEYED_NOW.decrementAndGet();
                return ServerResponse.ok().build();
              })
          .POST(
              "/stub/fast",
              request -> {
                ARRIVALS.add("fast:" + key(request));
                return ServerResponse.ok().build();
              })
          // the burst: every request waits at the gate
          .POST(
              "/stub/hold/{n}",
              request -> {
                int now = HELD_NOW.incrementAndGet();
                HELD_MAX.accumulateAndGet(now, Math::max);
                BURST_GATE.await(60, TimeUnit.SECONDS);
                HELD_NOW.decrementAndGet();
                return ServerResponse.ok().build();
              })
          .build();
    }

    /** The receiver's response time: a timed wait on a latch nobody opens. */
    private static void latency(Duration duration) throws InterruptedException {
      assertThat(new CountDownLatch(1).await(duration.toMillis(), TimeUnit.MILLISECONDS))
          .isFalse();
    }

    private static String key(ServerRequest request) {
      return request.headers().firstHeader(OutboxHeaders.IDEMPOTENCY_KEY);
    }

    /** Records where an HTTP row's booking ran: its transaction, its thread kind. */
    @Bean
    OutboxRelayedListener laneProbe() {
      return event -> {
        if (event.getDestination().startsWith("http")) {
          BOOKED_IN_TX.add(TransactionSynchronizationManager.isActualTransactionActive());
          BOOKED_ON_VIRTUAL.add(isVirtual(Thread.currentThread()));
        }
      };
    }
  }

  /** {@code Thread.isVirtual()} by reflection - the test sources compile for Java 17 too. */
  static boolean isVirtual(Thread thread) {
    try {
      Method isVirtual = Thread.class.getMethod("isVirtual");
      return (Boolean) isVirtual.invoke(thread);
    } catch (NoSuchMethodException ex) {
      return false; // pre-21 runtime: there are no virtual threads
    } catch (ReflectiveOperationException ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Autowired private OutboxWriter writer;
  @Autowired private OutboxMaintenanceService maintenance;
  @Autowired private TransactionTemplate tx;
  @Autowired private DataSource dataSource;
  @Autowired private MeterRegistry registry;
  @LocalServerPort private int port;

  private String url(String path) {
    return "http://localhost:" + port + "/stub/" + path;
  }

  private OutboxEvent append(String aggregateId, String destination) {
    return tx.execute(s -> writer.append("lane", aggregateId, "lane.v1", destination, Map.of()));
  }

  private int activeConnections() throws SQLException {
    return dataSource.unwrap(HikariDataSource.class).getHikariPoolMXBean().getActiveConnections();
  }

  private double inFlightGauge() {
    return registry.get("opentmf.outbox.in-flight").gauge().value();
  }

  /** Each test starts from a quiet lane - no row of an earlier test still in flight. */
  @BeforeEach
  void quietLane() {
    await().atMost(Duration.ofSeconds(60)).until(() -> inFlightGauge() == 0d);
  }

  /**
   * THE defect's own test (red on 1.2.x, where both rode one claim transaction on the single
   * relay thread and the Kafka row waited the full 30 s): an HTTP row whose subscriber answers
   * after 30 s does not delay a Kafka row appended AFTER it. And while that send is in flight,
   * no JDBC connection is checked out - the pool reads zero, sample after sample - and the row
   * shows as in flight on /ops and on the gauge.
   */
  @Test
  void aSlowHttpRow_doesNotDelayAnOrderedRowAppendedAfterIt_andHoldsNoConnection()
      throws Exception {
    OutboxEvent slow = append("a-slow", url("slow"));
    assertThat(SLOW_RECEIVED.await(20, TimeUnit.SECONDS)).isTrue(); // the send is in flight

    OutboxEvent ordered = append("a-kafka", "lane-" + UUID.randomUUID());

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(maintenance.inspect(ordered.getId()).relayedOn()).isNotNull());
    OutboxRowView inFlight = maintenance.inspect(slow.getId());
    assertThat(inFlight.relayedOn()).isNull(); // still answering...
    assertThat(inFlight.inFlight()).isTrue(); // ...and visibly so
    assertThat(inFlight.claimedUntil()).isNotNull();
    assertThat(inFlightGauge()).isEqualTo(1d);
    assertThat(registry.get("opentmf.outbox.pending").gauge().value()).isGreaterThanOrEqualTo(1d);

    // the pool, read through JMX (no connection taken to read it), polled over 3 s of the send
    await()
        .during(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(50))
        .until(() -> activeConnections() == 0);
    assertThat(maintenance.inspect(slow.getId()).relayedOn()).isNull(); // still in flight

    await()
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () -> assertThat(maintenance.inspect(slow.getId()).relayedOn()).isNotNull());
    assertThat(maintenance.inspect(slow.getId()).inFlight()).isFalse();
  }

  /**
   * Per-receiver order (the HTTP publisher keys on the destination): two rows to ONE slow
   * receiver arrive in id order and are never in flight together, while a row to ANOTHER
   * receiver, appended after both, overtakes them.
   */
  @Test
  void rowsToOneReceiver_arriveInIdOrder_whileAnotherReceiverOvertakesThem() {
    OutboxEvent first = append("a-1", url("keyed"));
    OutboxEvent second = append("a-2", url("keyed"));
    OutboxEvent other = append("a-3", url("fast"));

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> assertThat(maintenance.inspect(second.getId()).relayedOn()).isNotNull());

    List<String> mine =
        ARRIVALS.stream()
            .filter(
                arrival ->
                    List.of(first.getId(), second.getId(), other.getId()).stream()
                        .anyMatch(id -> arrival.endsWith(":outbox:" + id)))
            .toList();
    assertThat(mine)
        .containsExactly(
            "keyed:outbox-lane-it:outbox:" + first.getId(),
            "fast:outbox-lane-it:outbox:" + other.getId(),
            "keyed:outbox-lane-it:outbox:" + second.getId());
    assertThat(KEYED_MAX.get()).isEqualTo(1); // never two in flight to the one receiver
  }

  /**
   * The cap under a burst: 20 rows to 20 receivers, every receiver holding its request - exactly
   * {@code max-in-flight} (8) are ever in flight, the gauge says so, the rest are NOT leased
   * (no lease burns in a queue), and all 20 relay once the receivers answer.
   */
  @Test
  void theCap_holdsUnderABurst() {
    List<OutboxEvent> burst = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      int n = i;
      burst.add(append("b-" + n, url("hold/" + n)));
    }

    await().atMost(Duration.ofSeconds(20)).until(() -> HELD_NOW.get() == 8);
    await()
        .during(Duration.ofSeconds(2))
        .atMost(Duration.ofSeconds(5))
        .until(() -> HELD_NOW.get() == 8); // and it stays at the cap
    assertThat(inFlightGauge()).isEqualTo(8d);
    assertThat(burst)
        .filteredOn(row -> maintenance.inspect(row.getId()).claimedUntil() != null)
        .hasSize(8);

    BURST_GATE.countDown();
    await()
        .atMost(Duration.ofSeconds(60))
        .untilAsserted(
            () ->
                assertThat(burst)
                    .allSatisfy(
                        row ->
                            assertThat(maintenance.inspect(row.getId()).relayedOn())
                                .isNotNull()));
    assertThat(HELD_MAX.get()).isEqualTo(8);
  }

  /**
   * Where an HTTP row is booked: inside a transaction (the relayed listener's guarantee holds on
   * the CONCURRENT lane), on a lane thread of the kind the runtime and the application chose -
   * platform threads here, virtual ones in {@link OutboxHttpLaneVirtualIT}.
   */
  @Test
  void httpBookings_runInsideATransaction_onTheExpectedThreadKind() {
    OutboxEvent row = append("a-kind", url("fast"));

    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(
            () -> assertThat(maintenance.inspect(row.getId()).relayedOn()).isNotNull());

    assertThat(BOOKED_IN_TX).isNotEmpty().containsOnly(true);
    assertThat(BOOKED_ON_VIRTUAL).isNotEmpty().containsOnly(expectVirtualThreads());
  }

  /** Platform threads unless a subclass enables virtual ones. */
  boolean expectVirtualThreads() {
    return false;
  }
}
