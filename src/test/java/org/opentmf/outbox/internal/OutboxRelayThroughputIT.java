package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxTestApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.containers.Network;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

/**
 * The relay's THROUGHPUT on one pod (OUTBOX-RELAY-CLAIM-CADENCE-1), against a database that answers
 * every statement late: a Toxiproxy latency toxic on the application's link ({@code
 * -Dit.relay.latency-ms}, default 5 - the per-statement time measured on the box under load), so
 * each round trip of the relay costs what it costs in production and a statement per row shows.
 * 10,000 pending CONCURRENT rows over 5,000 ordering keys (two rows per key), a publisher that
 * takes 1 ms, ONE relay - built from the context's own beans exactly as the auto-configuration
 * wires it; the context's relay is parked (sweep 1h, rows seeded over a direct connection, so no
 * nudge fires). Every run also proves the key contract under load: no key ever has two rows in
 * flight, and each key's rows are sent in {@code id} order.
 *
 * <p>Measured on 1.5.0 (one statement per claimed row for the key lock, one for the stamp, the
 * claim bounded by the free slots) and on 1.6.0 - the figures are printed as {@code
 * THROUGHPUT-FIGURES} and recorded in the README.
 */
@Import(OutboxRelayThroughputIT.Publishers.class)
@SpringBootTest(
    classes = OutboxTestApplication.class,
    properties = {
      "spring.application.name=outbox-throughput-it",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "spring.datasource.hikari.maximum-pool-size=80", // never the bound at 64 in flight
      "opentmf.outbox.sweep-interval=1h"
    })
class OutboxRelayThroughputIT {

  private static final int ROWS = Integer.getInteger("it.relay.rows", 10_000);
  private static final int KEYS = Integer.getInteger("it.relay.keys", 5_000);
  private static final int LATENCY_MS = Integer.getInteger("it.relay.latency-ms", 5);

  static final Network NETWORK = Network.newNetwork();
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:18.1-alpine3.22")
          .withNetwork(NETWORK)
          .withNetworkAliases("postgres");
  static final ToxiproxyContainer TOXIPROXY =
      new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.12.0").withNetwork(NETWORK);
  static Proxy proxy;

  /** The keys with a row in flight right now, the per-key violations, and each key's last id. */
  static final Set<String> IN_FLIGHT = ConcurrentHashMap.newKeySet();

  static final Map<String, Long> LAST_SENT = new ConcurrentHashMap<>();
  static final AtomicInteger TWO_IN_FLIGHT = new AtomicInteger();
  static final AtomicInteger OUT_OF_ORDER = new AtomicInteger();

  @Autowired OutboxEventRepository repository;
  @Autowired OutboxPublisherRouter router;
  @Autowired OutboxMetrics metrics;
  @Autowired PlatformTransactionManager transactionManager;

  @BeforeAll
  static void containers() throws IOException {
    POSTGRES.start();
    TOXIPROXY.start();
    proxy =
        new ToxiproxyClient(TOXIPROXY.getHost(), TOXIPROXY.getControlPort())
            .createProxy("postgres", "0.0.0.0:8666", "postgres:5432");
  }

  @AfterAll
  static void stopContainers() {
    TOXIPROXY.stop();
    POSTGRES.stop();
    NETWORK.close();
  }

  /**
   * The application talks to the database through the proxy; the test seeds and counts directly.
   */
  @DynamicPropertySource
  static void datasource(DynamicPropertyRegistry registry) {
    registry.add(
        "spring.datasource.url",
        () ->
            "jdbc:postgresql://%s:%d/%s"
                .formatted(
                    TOXIPROXY.getHost(),
                    TOXIPROXY.getMappedPort(8666),
                    POSTGRES.getDatabaseName()));
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
  }

  @TestConfiguration
  static class Publishers {

    /** "golden:" rows: CONCURRENT, keyed by aggregate, a 15 s lease (Kafka's), each send 1 ms. */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    OutboxPublisher goldenPublisher() {
      return new OutboxPublisher() {
        @Override
        public boolean supports(OutboxEvent event) {
          return event.getDestination().startsWith("golden:");
        }

        @Override
        public Lane lane(OutboxEvent event) {
          return Lane.CONCURRENT;
        }

        @Override
        public String orderingKey(OutboxEvent event) {
          return event.getAggregateId();
        }

        @Override
        public Duration lease(OutboxEvent event) {
          return Duration.ofSeconds(15);
        }

        @Override
        public void publish(OutboxEvent event) {
          String key = event.getOrderingKey();
          if (!IN_FLIGHT.add(key)) {
            TWO_IN_FLIGHT.incrementAndGet();
          }
          Long last = LAST_SENT.put(key, event.getId());
          if (last != null && last > event.getId()) {
            OUT_OF_ORDER.incrementAndGet();
          }
          try { // the receiver's 1 ms: a timed wait on a latch nobody opens
            assertThat(new CountDownLatch(1).await(1, TimeUnit.MILLISECONDS)).isFalse();
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          } finally {
            IN_FLIGHT.remove(key);
          }
        }
      };
    }
  }

  @BeforeEach
  void seed() throws Exception {
    proxy.toxics().getAll().forEach(OutboxRelayThroughputIT::remove);
    IN_FLIGHT.clear();
    LAST_SENT.clear();
    TWO_IN_FLIGHT.set(0);
    OUT_OF_ORDER.set(0);
    try (Connection c = direct();
        Statement st = c.createStatement()) {
      st.execute("truncate outbox");
      st.execute(
          """
          insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
            created_on, attempts, next_attempt_on, lane, ordering_key)
          select 'golden', 'agg-' || (g %% %d), 'e', 'golden:topic', '{}', now(), 0,
            now() - interval '1 second', 'CONCURRENT', 'agg-' || (g %% %d)
          from generate_series(1, %d) g\
          """
              .formatted(KEYS, KEYS, ROWS));
      st.execute("vacuum analyze outbox");
    }
  }

  private static void remove(Toxic toxic) {
    try {
      toxic.remove();
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private static Connection direct() throws SQLException {
    return DriverManager.getConnection(
        POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
  }

  private static long pending() throws SQLException {
    try (Connection c = direct();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("select count(*) from outbox where relayed_on is null")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  /**
   * One relay with {@code maxInFlight} slots drains the seeded rows through the toxic; returns rows
   * per second, the time from the first poke to the last booking.
   */
  private double drain(int maxInFlight) throws Exception {
    OutboxProperties properties = new OutboxProperties();
    properties.setSweepInterval(Duration.ofHours(1)); // only the nudges drive it
    properties.getConcurrent().setMaxInFlight(maxInFlight);
    OutboxConcurrentLane lane =
        new OutboxConcurrentLane(
            maxInFlight, properties.getBatchSize(), false, properties.getShutdownGrace());
    OutboxOrderedLane orderedLane = new OutboxOrderedLane(properties.getShutdownGrace());
    OutboxRelayWorker worker =
        new OutboxRelayWorker(repository,
            router,
            new OutboxBackoff(properties),
            metrics,
            properties,
            List.of(),
            OutboxAutoConfiguration.relayTransactions(transactionManager),
            OutboxAutoConfiguration.laneTransactions(transactionManager),
            lane,
            orderedLane,
            OutboxClock.system(),
            OutboxSendHeartbeatFixture.idle());
    OutboxRelay relay = new OutboxRelay(worker, properties, lane, orderedLane, OutboxSendHeartbeatFixture.idle(), OutboxClock.system());
    proxy.toxics().latency("latency", ToxicDirection.DOWNSTREAM, LATENCY_MS);
    relay.start();
    try {
      long started = System.nanoTime();
      relay.poke();
      await()
          .atMost(Duration.ofMinutes(5))
          .pollInterval(Duration.ofMillis(50))
          .until(() -> pending() == 0);
      double seconds = (System.nanoTime() - started) / 1e9;
      double rate = ROWS / seconds;
      System.out.printf(
          "THROUGHPUT-FIGURES: %d rows, %d keys, latency %d ms, max-in-flight %d, batch-size %d:"
              + " %.1f s, %.0f rows/s%n",
          ROWS, KEYS, LATENCY_MS, maxInFlight, properties.getBatchSize(), seconds, rate);
      assertThat(TWO_IN_FLIGHT.get()).as("a key with two rows in flight").isZero();
      assertThat(OUT_OF_ORDER.get()).as("a key's rows out of id order").isZero();
      return rate;
    } finally {
      relay.stop();
    }
  }

  /** The acceptance on the box's shape: the DEFAULT 8 slots move at least 500 rows a second. */
  @Test
  void oneRelay_eightSlots_movesAtLeast500RowsPerSecond() throws Exception {
    assertThat(drain(8)).isGreaterThanOrEqualTo(500);
  }

  /** The golden figure: with 64 slots the 10,000 rows are gone in under 15 seconds. */
  @Test
  void oneRelay_sixtyFourSlots_drainsTenThousandRowsUnderFifteenSeconds() throws Exception {
    assertThat(drain(64)).isGreaterThanOrEqualTo(ROWS / 15.0);
  }
}
