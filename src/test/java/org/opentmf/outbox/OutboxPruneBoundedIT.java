package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.micrometer.core.instrument.MeterRegistry;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

/**
 * OTMF-OUTBOX-PRUNE-STATEMENT-BOUNDED-AND-PLANNED-1: what bounds the retention prune, and what a
 * caller sees when the database cancels one of its batches - measured on a real database, through
 * the application's own pool, which here carries a {@code statement_timeout} of 2 s as an adopter's
 * prune path may.
 *
 * <p>A statement-level trigger on {@code outbox} records the rows each delete statement removed
 * ({@code prune_log}, written in the statement's own transaction, so only a batch that committed
 * leaves a line) and, when the case asks for it, holds the n-th statement ({@code prune_stall}): a
 * slow batch, made on purpose.
 *
 * <ul>
 *   <li>ONE STATEMENT deletes at most {@code maintenance.batch-size} rows;
 *   <li>ONE CALL runs batches until none are left or {@code maintenance.time-budget} is spent - it
 *       is not one batch per call; a budget that one batch spends runs exactly one;
 *   <li>a batch the database cancels (SQLSTATE 57014) ends the call with Spring's {@link
 *       QueryTimeoutException}; that batch is rolled back whole, the batches before it stay deleted
 *       and are on the {@code opentmf.outbox.pruned} counter, and the next call continues with the
 *       oldest expired rows.
 * </ul>
 */
@SpringBootTest(
    properties = {
      "spring.application.name=outbox-prune-bounded-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///prunebounded",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.datasource.hikari.connection-init-sql=set statement_timeout = '2s'",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=1h",
      "opentmf.outbox.maintenance.batch-size=1000"
    })
@DirtiesContext
class OutboxPruneBoundedIT {

  private static final int BATCH = 1_000;
  private static final int EXPIRED = 2_500;

  @Autowired private OutboxMaintenanceService maintenance;
  @Autowired private OutboxProperties properties;
  @Autowired private JdbcTemplate jdbc;
  @Autowired private MeterRegistry registry;

  private Duration budget;

  /**
   * 2,500 relayed rows past the retention (oldest first by {@code relayed_on}), and beside them a
   * fresh relayed row, a parked row and a held open row the prune must never touch.
   */
  @BeforeEach
  void anExpiredBacklog() {
    budget = properties.getMaintenance().getTimeBudget();
    jdbc.execute("truncate outbox restart identity");
    jdbc.execute(
        """
        create table if not exists prune_log (
          seq bigint generated always as identity, deleted int not null)\
        """);
    jdbc.execute("create table if not exists prune_stall (statement int, seconds numeric)");
    jdbc.execute("truncate prune_log, prune_stall");
    jdbc.execute(
        """
        create or replace function prune_logged() returns trigger language plpgsql as $$
        declare n int; s numeric;
        begin
          select count(*) + 1 into n from prune_log;
          insert into prune_log (deleted) select count(*) from gone;
          select seconds into s from prune_stall where statement = n;
          if s is not null then perform pg_sleep(s); end if;
          return null;
        end $$\
        """);
    jdbc.execute("drop trigger if exists prune_logged on outbox");
    jdbc.execute(
        """
        create trigger prune_logged after delete on outbox referencing old table as gone
        for each statement execute function prune_logged()\
        """);
    jdbc.update(
        """
        insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
          created_on, attempts, next_attempt_on, relayed_on)
        select 't', 'x' || g, 'e', 'd', '{}', now() - interval '30 days', 1, now(),
          now() - interval '20 days' + g * interval '1 second'
        from generate_series(1, ?) g\
        """,
        EXPIRED);
    jdbc.execute(
        """
        insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
          created_on, attempts, next_attempt_on, relayed_on, parked_on, release_at)
        values ('t', 'fresh', 'e', 'd', '{}', now(), 1, now(), now(), null, null),
          ('t', 'parked', 'e', 'd', '{}', now() - interval '30 days', 10, now(), null,
            now() - interval '20 days', null),
          ('t', 'held', 'e', 'd', '{}', now() - interval '30 days', 0, now(), null, null,
            now() + interval '1 day')\
        """);
  }

  @AfterEach
  void theBudgetAsItWas() {
    properties.getMaintenance().setTimeBudget(budget);
    jdbc.execute("drop trigger if exists prune_logged on outbox");
  }

  private List<Integer> committedStatements() {
    return jdbc.queryForList("select deleted from prune_log order by seq", Integer.class);
  }

  private long expiredLeft() {
    return count("relayed_on < now() - interval '7 days'");
  }

  private long count(String where) {
    return jdbc.queryForObject("select count(*) from outbox where " + where, Long.class);
  }

  private double prunedRelayed() {
    return registry.get("opentmf.outbox.pruned").tag("state", "relayed").counter().count();
  }

  private void untouched() {
    assertThat(count("aggregate_id in ('fresh', 'parked', 'held')"))
        .as("the fresh, parked and held rows are never pruned")
        .isEqualTo(3);
  }

  @Test
  void oneStatement_deletesAtMostTheBatchSize_andOneCall_runsBatchesUntilNoneAreLeft() {
    OutboxPruneResult result = maintenance.pruneExpired();

    assertThat(result.relayed()).isEqualTo(EXPIRED);
    assertThat(result.moreToPrune()).isFalse();
    assertThat(committedStatements())
        .as("one call, several statements: 1000, 1000, 500 relayed, then the cancelled leg")
        .containsExactly(BATCH, BATCH, EXPIRED - 2 * BATCH, 0)
        .allMatch(deleted -> deleted <= BATCH);
    assertThat(expiredLeft()).isZero();
    untouched();
  }

  @Test
  void aCallWhoseBudgetOneBatchSpends_runsExactlyOneBatch_andSaysMoreRemain() {
    properties.getMaintenance().setTimeBudget(Duration.ofMillis(500));
    jdbc.update("insert into prune_stall values (1, 1.0)"); // the first batch takes a second

    OutboxPruneResult result = maintenance.pruneExpired();

    assertThat(result.relayed()).isEqualTo(BATCH);
    assertThat(result.moreToPrune()).isTrue();
    assertThat(committedStatements()).containsExactly(BATCH);
    assertThat(expiredLeft()).isEqualTo(EXPIRED - BATCH);
  }

  @Test
  void aCancelledBatch_endsTheCall_rolledBackWhole_andTheNextCallContinues() {
    double before = prunedRelayed();
    jdbc.update("insert into prune_stall values (2, 5.0)"); // the second batch outlasts 2 s

    Throwable cancelled = catchThrowable(maintenance::pruneExpired);

    System.out.println("PRUNE-CANCEL: the caller sees " + cancelled);
    assertThat(cancelled)
        .as("the caller sees Spring's QueryTimeoutException, the statement's cancel as its cause")
        .isInstanceOf(QueryTimeoutException.class)
        .rootCause()
        .isInstanceOf(SQLException.class);
    assertThat(((SQLException) rootCause(cancelled)).getSQLState()).isEqualTo("57014");
    assertThat(committedStatements())
        .as("the first batch committed; the cancelled one left no line")
        .containsExactly(BATCH);
    assertThat(expiredLeft())
        .as("the cancelled batch is rolled back whole: every one of its rows is still there")
        .isEqualTo(EXPIRED - BATCH);
    assertThat(count("aggregate_id in ('x1', 'x1000')")).as("the oldest batch is gone").isZero();
    assertThat(prunedRelayed() - before)
        .as("the call answered nothing, the counter has the first batch's rows")
        .isEqualTo(BATCH);

    jdbc.update("truncate prune_stall");
    OutboxPruneResult next = maintenance.pruneExpired();

    assertThat(next.relayed()).as("the next call continues").isEqualTo(EXPIRED - BATCH);
    assertThat(next.moreToPrune()).isFalse();
    assertThat(expiredLeft()).isZero();
    assertThat(prunedRelayed() - before).isEqualTo(EXPIRED);
    untouched();
  }

  private static Throwable rootCause(Throwable t) {
    Throwable cause = t;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }
}
