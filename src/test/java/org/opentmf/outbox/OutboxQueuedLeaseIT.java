package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * OUTBOX-QUEUED-LEASE-STARVATION-1 (1.8.1): a CONCURRENT row never waits in the hand-off queue
 * behind slow sends while its lease runs out. Up to 1.8.0 the claim ran up to {@code batch-size}
 * rows ahead of the slots, so with every slot held by a slow send a short-lease row waited in the
 * queue, its lease lapsed, the next pass claimed it AGAIN and queued it at the tail - measured on a
 * flow service: no Kafka row (15 s lease) relayed for six minutes behind HTTP sends of about 36 s.
 *
 * <p>Here the eight slots are held by sends that wait at the test's gate (a two-minute lease); a
 * row with a ONE-second lease is appended behind them and waits three seconds - three sweeps, three
 * chances to claim it again. It is stamped at most once before its send, and it goes out as soon as
 * one slot frees.
 */
@SpringBootTest(
    properties = {
      "spring.application.name=outbox-queued-lease-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///queued",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=1s",
      "opentmf.outbox.concurrent.max-in-flight=8"
    })
class OutboxQueuedLeaseIT {

  /** One permit per slow send the test lets finish. */
  static final Semaphore SLOW_GATE = new Semaphore(0);

  static final AtomicInteger SLOW_SENDING = new AtomicInteger();
  static final Map<Long, AtomicInteger> FAST_DELIVERED = new ConcurrentHashMap<>();
  static final Map<Long, Long> FAST_SENT_AT = new ConcurrentHashMap<>();

  @TestConfiguration
  static class Receivers {

    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
      return new TransactionTemplate(txManager);
    }

    /** "slow:" rows hold their slot until the gate lets them go; "fast:" rows have a 1 s lease. */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    OutboxPublisher receivers() {
      return new OutboxPublisher() {
        @Override
        public boolean supports(OutboxEvent event) {
          return event.getDestination().matches("^(slow|fast):.*");
        }

        @Override
        public Lane lane(OutboxEvent event) {
          return Lane.CONCURRENT;
        }

        @Override
        public String orderingKey(OutboxEvent event) {
          return event.getDestination();
        }

        @Override
        public Duration lease(OutboxEvent event) {
          return event.getDestination().startsWith("fast:")
              ? Duration.ofSeconds(1)
              : Duration.ofMinutes(2);
        }

        @Override
        public void publish(OutboxEvent event) {
          if (event.getDestination().startsWith("fast:")) {
            FAST_SENT_AT.putIfAbsent(event.getId(), System.nanoTime());
            FAST_DELIVERED
                .computeIfAbsent(event.getId(), id -> new AtomicInteger())
                .incrementAndGet();
            return;
          }
          SLOW_SENDING.incrementAndGet();
          try {
            assertThat(SLOW_GATE.tryAcquire(60, TimeUnit.SECONDS)).isTrue();
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          } finally {
            SLOW_SENDING.decrementAndGet();
          }
        }
      };
    }
  }

  @Autowired private OutboxWriter writer;
  @Autowired private TransactionTemplate tx;
  @Autowired private JdbcTemplate jdbc;

  private OffsetDateTime claimedUntil(long id) {
    return jdbc.queryForObject(
        "select claimed_until from outbox where id = ?", OffsetDateTime.class, id);
  }

  @Test
  void aShortLeaseRowBehindBusySlots_isNeverClaimedAgain_andGoesWhenASlotFrees() {
    try {
      tx.executeWithoutResult(
          status ->
              IntStream.range(0, 8)
                  .forEach(
                      i ->
                          writer.append(
                              "it-aggregate", "s-" + i, "it.slow.v1", "slow:" + i, Map.of())));
      await().atMost(Duration.ofSeconds(10)).until(() -> SLOW_SENDING.get() == 8); // every slot

      long fast =
          tx.execute(
              status ->
                  writer
                      .append("it-aggregate", "f-1", "it.fast.v1", "fast:urgent", Map.of())
                      .getId());

      // three seconds behind the busy slots - three times its lease, three sweeps
      Set<OffsetDateTime> stamps = ConcurrentHashMap.newKeySet();
      List<Long> polls = new CopyOnWriteArrayList<>();
      await()
          .pollInterval(Duration.ofMillis(100))
          .during(Duration.ofSeconds(3))
          .atMost(Duration.ofSeconds(4))
          .until(
              () -> {
                OffsetDateTime stamp = claimedUntil(fast);
                if (stamp != null) {
                  stamps.add(stamp);
                }
                polls.add(System.nanoTime());
                return !FAST_DELIVERED.containsKey(fast); // no slot was free
              });
      assertThat(stamps)
          .as("leases stamped on the waiting row - never claimed again")
          .hasSizeLessThanOrEqualTo(1);

      long freed = System.nanoTime();
      SLOW_GATE.release(); // one slot frees

      await().atMost(Duration.ofSeconds(1)).until(() -> FAST_DELIVERED.containsKey(fast));
      System.out.printf(
          "QUEUED-LEASE: the 1 s-lease row went out %d ms after a slot freed, stamped %d time(s)"
              + " while it waited%n",
          (FAST_SENT_AT.get(fast) - freed) / 1_000_000, stamps.size());
      await()
          .during(Duration.ofSeconds(2))
          .atMost(Duration.ofSeconds(3))
          .until(() -> FAST_DELIVERED.get(fast).get() == 1); // exactly once
    } finally {
      SLOW_GATE.release(100);
    }
  }
}
