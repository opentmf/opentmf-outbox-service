package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * OUTBOX-BULK-UNPARK-1: the unpark by FILTER on the real engine. Thousands of parked rows of two
 * receivers: only the named receiver's rows inside the parked-on range leave the parked state,
 * in bounded batches, each exactly as the single unpark leaves it; the other receiver's rows and
 * the rows outside the range stay parked; what the relay then SENDS stays bounded by the lanes
 * (CONCURRENT: max-in-flight; ORDERED: batch-size per pass); and a row an ops action holds is
 * waited for and re-checked, as for the single unpark.
 */
@SpringBootTest(
    properties = {
      "spring.application.name=outbox-unpark-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///unpark",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=1s",
      "opentmf.outbox.batch-size=50",
      "opentmf.outbox.maintenance.batch-size=500"
    })
class OutboxBulkUnparkIT {

  static final AtomicInteger CONCURRENT_NOW = new AtomicInteger();
  static final AtomicInteger CONCURRENT_MAX = new AtomicInteger();
  static final CountDownLatch GATE = new CountDownLatch(1);

  @TestConfiguration
  static class Receivers {

    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
      return new TransactionTemplate(txManager);
    }

    /** "conc:" rows wait at the test's gate (CONCURRENT); "ord:" rows answer in 5 ms. */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    OutboxPublisher receivers() {
      return new OutboxPublisher() {
        @Override
        public boolean supports(OutboxEvent event) {
          return event.getDestination().matches("^(conc|ord):.*");
        }

        @Override
        public Lane lane(OutboxEvent event) {
          return event.getDestination().startsWith("conc:") ? Lane.CONCURRENT : Lane.ORDERED;
        }

        @Override
        public void publish(OutboxEvent event) {
          try {
            if (event.getDestination().startsWith("conc:")) {
              CONCURRENT_MAX.accumulateAndGet(CONCURRENT_NOW.incrementAndGet(), Math::max);
              try {
                assertThat(GATE.await(60, TimeUnit.SECONDS)).isTrue();
              } finally {
                CONCURRENT_NOW.decrementAndGet();
              }
            } else { // the receiver's response time
              assertThat(new CountDownLatch(1).await(5, TimeUnit.MILLISECONDS)).isFalse();
            }
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
      };
    }
  }

  @Autowired private OutboxMaintenanceService maintenance;
  @Autowired private JdbcTemplate jdbc;
  @Autowired private TransactionTemplate tx;

  /** Parked rows as the relay leaves them: budget spent, parked_on stamped, the error kept. */
  private void park(String destination, String lane, int count, String parkedOn) {
    jdbc.execute(
        """
        insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
          created_on, attempts, next_attempt_on, parked_on, last_error, lane)
        select 't', 'a', 'e', '%s', '{}', now() - interval '2 hours', 10,
          now() - interval '1 hour', %s, '503 - receiver down', '%s'
        from generate_series(1, %d)"""
            .formatted(destination, parkedOn, lane, count));
  }

  private long count(String where) {
    return jdbc.queryForObject("select count(*) from outbox where " + where, Long.class);
  }

  @Test
  void onlyTheNamedReceiversRowsInTheRange_leaveTheParkedState_andTheLanesBoundTheResend() {
    park("conc:receiver-a", "CONCURRENT", 1_500, "now() - interval '30 minutes'"); // in range
    park("conc:receiver-a", "CONCURRENT", 500, "now() - interval '3 days'"); // before it
    park("conc:receiver-b", "CONCURRENT", 1_000, "now() - interval '30 minutes'"); // other
    OffsetDateTime from = OffsetDateTime.now(ZoneOffset.UTC).minusHours(2);
    OffsetDateTime to = OffsetDateTime.now(ZoneOffset.UTC).plusMinutes(1);

    OutboxUnparkResult result = maintenance.unpark("conc:receiver-a", from, to, null);

    assertThat(result.unparked()).isEqualTo(1_500); // three full batches of 500, then none
    assertThat(result.moreToUnpark()).isFalse();
    assertThat(count("destination = 'conc:receiver-a' and parked_on is null")).isEqualTo(1_500);
    assertThat(count("destination = 'conc:receiver-a' and parked_on is not null")).isEqualTo(500);
    assertThat(count("destination = 'conc:receiver-b' and parked_on is not null"))
        .isEqualTo(1_000);
    // each exactly as the single unpark leaves it: attempts reset, due, the error kept, no lease
    assertThat(
            count(
                "destination = 'conc:receiver-a' and parked_on is null and (attempts <> 0"
                    + " or last_error is null or next_attempt_on > now() + interval '1 minute')"
                    + " and relayed_on is null and claimed_until is null"))
        .isZero();

    // the resend is bounded by the CONCURRENT lane: max-in-flight (8), however many came back
    await().atMost(Duration.ofSeconds(20)).until(() -> CONCURRENT_NOW.get() == 8);
    await()
        .during(Duration.ofSeconds(2))
        .atMost(Duration.ofSeconds(4))
        .until(() -> CONCURRENT_NOW.get() <= 8);
    assertThat(CONCURRENT_MAX.get()).isEqualTo(8);
    GATE.countDown();
    await()
        .atMost(Duration.ofSeconds(60))
        .until(
            () -> count("destination = 'conc:receiver-a' and relayed_on is not null") == 1_500);
    assertThat(count("destination = 'conc:receiver-b' and parked_on is not null"))
        .isEqualTo(1_000); // still parked: never named
  }

  @Test
  void theOrderedLane_takesAtMostItsBatchPerPass_afterABulkUnpark() {
    park("ord:topic-c", "ORDERED", 300, "now() - interval '10 minutes'");

    OutboxUnparkResult result = maintenance.unpark("ord:topic-c", null, null, null);

    assertThat(result.unparked()).isEqualTo(300);
    int maxLeased = 0;
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (count("destination = 'ord:topic-c' and relayed_on is null") > 0
        && System.nanoTime() < deadline) {
      maxLeased =
          (int)
              Math.max(
                  maxLeased,
                  count(
                      "destination = 'ord:topic-c' and claimed_until > now()"
                          + " and relayed_on is null"));
    }
    assertThat(count("destination = 'ord:topic-c' and relayed_on is null")).isZero();
    assertThat(maxLeased).isPositive().isLessThanOrEqualTo(50); // batch-size per pass
  }

  @Test
  void aReferenceNarrowsTheFilter_andTheDestinationIsRequired() {
    park("ord:topic-d", "ORDERED", 20, "now() - interval '10 minutes'");
    jdbc.update(
        "update outbox set reference = 'sub-7' where id in (select id from outbox"
            + " where destination = 'ord:topic-d' order by id limit 5)");
    OutboxUnparkResult result = maintenance.unpark("ord:topic-d", null, null, "sub-7");

    assertThat(result.unparked()).isEqualTo(5);
    assertThat(
            count("destination = 'ord:topic-d' and reference is null and parked_on is not null"))
        .isEqualTo(15);
    assertThatIllegalArgumentException()
        .isThrownBy(() -> maintenance.unpark(" ", null, null, null))
        .withMessageContaining("destination");
  }

  /**
   * The same guard as the single unpark: a row an ops action holds is WAITED for, then re-checked
   * as that action left it - cancelled meanwhile, it stays cancelled and is not unparked.
   */
  @Test
  void aRowAnOpsActionHolds_isWaitedFor_andReChecked() throws Exception {
    park("ord:topic-e", "ORDERED", 1, "now() - interval '10 minutes'");
    long id =
        jdbc.queryForObject(
            "select id from outbox where destination = 'ord:topic-e'", Long.class);
    CountDownLatch holding = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CompletableFuture<Void> opsAction =
        CompletableFuture.runAsync(
            () ->
                tx.executeWithoutResult(
                    s -> {
                      jdbc.queryForObject(
                          "select id from outbox where id = ? for update", Long.class, id);
                      holding.countDown();
                      try {
                        assertThat(release.await(20, TimeUnit.SECONDS)).isTrue();
                      } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                      }
                      jdbc.update("update outbox set cancelled_on = now() where id = ?", id);
                    }));
    assertThat(holding.await(10, TimeUnit.SECONDS)).isTrue();

    CompletableFuture<OutboxUnparkResult> unpark =
        CompletableFuture.supplyAsync(() -> maintenance.unpark("ord:topic-e", null, null, null));
    await() // waiting behind the ops action, as the single unpark would
        .during(Duration.ofMillis(500))
        .atMost(Duration.ofSeconds(2))
        .until(() -> !unpark.isDone());
    release.countDown(); // the ops action cancels the row and commits
    opsAction.get(10, TimeUnit.SECONDS);

    assertThat(unpark.get(10, TimeUnit.SECONDS).unparked()).isZero();
    assertThat(count("id = " + id + " and cancelled_on is not null and parked_on is not null"))
        .isEqualTo(1);
  }
}
