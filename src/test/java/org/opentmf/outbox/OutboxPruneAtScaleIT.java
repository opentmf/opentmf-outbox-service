package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.persistence.EntityManagerFactory;
import java.time.Duration;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * F-1 (load test 2026-10-06): the prune at scale. Several hundred thousand expired rows beside
 * fresh, parked and pending ones: the prune deletes set-based - NOT ONE entity is loaded
 * (Hibernate statistics) - and touches nothing fresh, parked or pending. Up to 1.2.1 it loaded
 * every expired row as an entity in one transaction and killed the pods that ran it.
 */
@SpringBootTest(
    properties = {
      "spring.application.name=outbox-prune-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///prune",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "spring.jpa.properties.hibernate.generate_statistics=true",
      "opentmf.outbox.sweep-interval=1h",
      "opentmf.outbox.metrics-refresh=200ms"
    })
class OutboxPruneAtScaleIT {

  static final int EXPIRED_RELAYED = 300_000;
  static final int EXPIRED_CANCELLED = 5_000;

  @Autowired private OutboxMaintenanceService maintenance;
  @Autowired private JdbcTemplate jdbc;
  @Autowired private EntityManagerFactory entityManagerFactory;
  @Autowired private OutboxProperties properties;
  @Autowired private MeterRegistry registry;

  private Statistics statistics() {
    return entityManagerFactory.unwrap(SessionFactory.class).getStatistics();
  }

  private long count(String where) {
    return jdbc.queryForObject("select count(*) from outbox where " + where, Long.class);
  }

  @BeforeEach
  void seed() {
    jdbc.execute("delete from outbox");
    String insert =
        "insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,"
            + " created_on, attempts, next_attempt_on, relayed_on, cancelled_on, parked_on)"
            + " select 't', 'a', 'e', 'topic', '{}', %s, 0, %s, %s, %s, %s"
            + " from generate_series(1, %d)";
    String old = "now() - interval '30 days'";
    jdbc.execute(insert.formatted(old, old, old, "null", "null", EXPIRED_RELAYED)); // expired
    jdbc.execute(insert.formatted(old, old, "null", old, "null", EXPIRED_CANCELLED)); // expired
    jdbc.execute(insert.formatted(old, old, "now() - interval '1 day'", "null", "null", 1_000));
    jdbc.execute(insert.formatted(old, old, "null", "null", old, 100)); // parked: never pruned
    jdbc.execute(insert.formatted(old, old, "null", "null", "null", 100)); // pending
    jdbc.execute("vacuum analyze outbox");
  }

  @Test
  void theExpiredRowsGo_setBased_noEntityLoaded_nothingFreshTouched() {
    statistics().clear();

    long pruned = 0;
    int calls = 0;
    long started = System.nanoTime();
    OutboxPruneResult result;
    do {
      result = maintenance.pruneExpired();
      pruned += result.pruned();
      calls++;
    } while (result.moreToPrune() && calls < 100);
    long millis = (System.nanoTime() - started) / 1_000_000;
    System.out.printf(
        "PRUNE-AT-SCALE: %d rows in %d call(s), %d ms%n", pruned, calls, millis); // PR evidence

    assertThat(pruned).isEqualTo(EXPIRED_RELAYED + EXPIRED_CANCELLED);
    assertThat(result.moreToPrune()).isFalse();
    assertThat(statistics().getEntityLoadCount()).isZero(); // set-based: nothing in the heap
    assertThat(statistics().getEntityDeleteCount()).isZero();
    assertThat(count("relayed_on is not null")).isEqualTo(1_000); // the fresh ones stay
    assertThat(count("parked_on is not null")).isEqualTo(100);
    assertThat(count("relayed_on is null and cancelled_on is null and parked_on is null"))
        .isEqualTo(100);
  }

  /**
   * A call is BOUNDED: with a short time budget it stops after a few batches, answers how many
   * it pruned and that more remain - a request thread never holds the pod for the whole backlog.
   */
  @Test
  void aCall_isBoundedByItsTimeBudget_andSaysMoreRemain() {
    Duration budget = properties.getMaintenance().getTimeBudget();
    properties.getMaintenance().setTimeBudget(Duration.ofMillis(1));
    try {
      OutboxPruneResult first = maintenance.pruneExpired();

      assertThat(first.moreToPrune()).isTrue();
      assertThat(first.pruned()).isPositive().isLessThanOrEqualTo(5L * 5_000);
      assertThat(count("relayed_on < now() - interval '7 days'"))
          .isEqualTo(EXPIRED_RELAYED - first.relayed());
    } finally {
      properties.getMaintenance().setTimeBudget(budget);
    }
  }

  /**
   * The gauges against the REAL database, beside 305,000 terminal rows: refreshed off the scrape
   * path, they read the open rows exactly - 100 pending + 100 parked, the oldest open for ~30 days
   * - never NaN once a refresh has run.
   */
  @Test
  void theGauges_readTheOpenRowsExactly_fromTheRefreshedSnapshot() {
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              assertThat(registry.get("opentmf.outbox.pending").gauge().value()).isEqualTo(200d);
              assertThat(registry.get("opentmf.outbox.parked").gauge().value()).isEqualTo(100d);
              assertThat(registry.get("opentmf.outbox.in-flight").gauge().value()).isZero();
              assertThat(registry.get("opentmf.outbox.relay-lag").gauge().value())
                  .isBetween(29.9 * 86_400, 30.1 * 86_400);
            });
  }
}
