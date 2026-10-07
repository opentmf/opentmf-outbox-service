package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The lanes AT SCALE, on the real claim queries: a slow but ALIVE subscriber - one ordering key,
 * its rows going one at a time - must not keep a Kafka-lane row out of reach however long its
 * backlog grows (dnms-681: one hub row per subscriber per event, interleaved with the Kafka
 * rows); and a row of a key that comes due again while a LATER row of its key is in flight is
 * not sent beside it ("never in flight together").
 */
@SpringBootTest(
    properties = {
      "spring.application.name=outbox-backlog-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///backlog",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=1s",
      "opentmf.outbox.shutdown-grace=1s"
    })
class OutboxBacklogIT {

  /** Deliveries in arrival order, per row id. */
  static final Map<Long, AtomicInteger> DELIVERED = new ConcurrentHashMap<>();

  /** What the "keyed" receiver is serving right now (row ids). */
  static final List<Long> KEYED_IN_FLIGHT = new CopyOnWriteArrayList<>();

  static final AtomicInteger KEYED_MAX_IN_FLIGHT = new AtomicInteger();
  static final CountDownLatch KEYED_GATE = new CountDownLatch(1);

  @TestConfiguration
  static class Publishers {

    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
      return new TransactionTemplate(txManager);
    }

    /**
     * {@code slow:} - ONE subscriber answering in 10 s, one ordering key; {@code keyed:} - one
     * key, the FIRST attempt of the first row fails (backoff 2 s), every later send waits at the
     * test's gate; {@code ord:} - an ORDERED (Kafka-lane) row.
     */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    OutboxPublisher backlogPublisher() {
      return new OutboxPublisher() {
        @Override
        public boolean supports(OutboxEvent event) {
          return event.getDestination().matches("^(slow|keyed|ord):.*");
        }

        @Override
        public Lane lane(OutboxEvent event) {
          return event.getDestination().startsWith("ord:") ? Lane.ORDERED : Lane.CONCURRENT;
        }

        @Override
        public String orderingKey(OutboxEvent event) {
          return event.getDestination().startsWith("ord:") ? null : event.getDestination();
        }

        @Override
        public Duration backoff(OutboxEvent event, int attempt) {
          return Duration.ofSeconds(2);
        }

        @Override
        public void publish(OutboxEvent event) {
          int tries =
              DELIVERED.computeIfAbsent(event.getId(), id -> new AtomicInteger()).incrementAndGet();
          String destination = event.getDestination();
          if (destination.startsWith("slow:")) {
            latency(Duration.ofSeconds(10));
          } else if (destination.startsWith("keyed:")) {
            keyed(event, tries);
          }
        }

        private void keyed(OutboxEvent event, int tries) {
          if (event.getAggregateId().equals("first") && tries == 1) {
            throw new IllegalStateException("503 - try later"); // backs off 2 s
          }
          KEYED_IN_FLIGHT.add(event.getId());
          KEYED_MAX_IN_FLIGHT.accumulateAndGet(KEYED_IN_FLIGHT.size(), Math::max);
          try {
            assertThat(KEYED_GATE.await(60, TimeUnit.SECONDS)).isTrue();
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          } finally {
            KEYED_IN_FLIGHT.remove(event.getId());
          }
        }
      };
    }
  }

  /** The receiver's response time: a timed wait on a latch nobody opens. */
  static void latency(Duration duration) {
    try {
      assertThat(new CountDownLatch(1).await(duration.toMillis(), TimeUnit.MILLISECONDS))
          .isFalse();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  @Autowired private OutboxWriter writer;
  @Autowired private OutboxMaintenanceService maintenance;
  @Autowired private TransactionTemplate tx;

  /**
   * THE scale test (red on PR #6 as first built, where the claim scanned at most ten windows of
   * candidates and an ORDERED row behind 1,000 of them was out of reach): 1,500 pending rows of
   * ONE slow subscriber, then one ORDERED row - it relays within seconds.
   */
  @Test
  void anOrderedRow_relaysWithinSeconds_behind1500RowsOfOneSlowSubscriber() {
    tx.executeWithoutResult(
        s -> {
          for (int i = 0; i < 1500; i++) {
            writer.append("hub", "sub-slow", "hub.v1", "slow:subscriber-1", Map.of("n", i));
          }
        });

    OutboxEvent ordered =
        tx.execute(s -> writer.append("comm", "c-1", "comm.v1", "ord:topic", Map.of()));

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(maintenance.inspect(ordered.getId()).relayedOn()).isNotNull());
  }

  /**
   * "Never in flight together": the first row of a key fails and backs off, the second passes
   * it (accepted - a failed row gives up order) and is now IN FLIGHT; when the first comes due
   * again it must NOT be sent beside the second - only after the second is booked.
   */
  @Test
  void aRowThatComesDueAgain_waitsWhileALaterRowOfItsKeyIsInFlight() {
    OutboxEvent first =
        tx.execute(s -> writer.append("hub", "first", "hub.v1", "keyed:subscriber-2", Map.of()));
    OutboxEvent second =
        tx.execute(s -> writer.append("hub", "second", "hub.v1", "keyed:subscriber-2", Map.of()));

    await().atMost(Duration.ofSeconds(10)).until(() -> KEYED_IN_FLIGHT.contains(second.getId()));
    // the first row's 2 s backoff passes several sweeps over while the second is in flight
    await()
        .during(Duration.ofSeconds(4))
        .atMost(Duration.ofSeconds(6))
        .until(() -> DELIVERED.get(first.getId()).get() == 1);

    KEYED_GATE.countDown();
    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(
            () -> assertThat(maintenance.inspect(first.getId()).relayedOn()).isNotNull());
    assertThat(KEYED_MAX_IN_FLIGHT.get()).isEqualTo(1);
  }
}
