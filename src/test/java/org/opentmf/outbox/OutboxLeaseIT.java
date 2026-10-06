package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxBooking.Outcome;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The LEASE contract on the real engine (1.3.0), through a consumer publisher that serves both
 * lanes ({@code <scenario>-ordered:} / {@code <scenario>-concurrent:} destinations): the send
 * runs in no transaction and holds no connection; the booking hook sees EVERY outcome and its
 * writes commit with the row's bookkeeping and roll back with it; the relayed listener runs in
 * the transaction that stamps {@code relayed_on} on both lanes; a lapsed lease is re-claimed and
 * the late holder books nothing; a cancel does not wait for a send in flight and the booking
 * honours it.
 */
@SpringBootTest(
    properties = {
      "spring.application.name=outbox-lease-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///lease",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog-lease.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=1s",
      // the gauges follow quickly here: these tests read in-flight as their quiet signal
      "opentmf.outbox.metrics-refresh=200ms"
    })
class OutboxLeaseIT {

  /** Deliveries per row, with the idempotency key each carried. */
  static final Map<Long, List<String>> DELIVERED = new ConcurrentHashMap<>();

  /** Outcomes the booking hook saw, per row, in order. */
  static final Map<Long, List<Outcome>> HOOKED = new ConcurrentHashMap<>();

  /** Counted down when the lapse scenario's FIRST (late) holder gets its answer. */
  static final CountDownLatch LATE_ANSWERED = new CountDownLatch(1);

  /** Rows whose relayed listener has refused once already. */
  static final Set<Long> REFUSED_ONCE = ConcurrentHashMap.newKeySet();

  /** Relayed-listener calls per row. */
  static final Map<Long, AtomicInteger> LISTENED = new ConcurrentHashMap<>();

  /** Per delivery: was a transaction active / how many pool connections were checked out. */
  static final List<Boolean> SENT_IN_TX = new CopyOnWriteArrayList<>();
  static final Map<Long, Integer> ACTIVE_AT_SEND = new ConcurrentHashMap<>();

  static final CountDownLatch GATE = new CountDownLatch(1);
  static final CountDownLatch GATE_REACHED = new CountDownLatch(2);

  @TestConfiguration
  static class Consumer {

    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
      return new TransactionTemplate(txManager);
    }

    @Bean
    JdbcTemplate jdbcTemplate(DataSource dataSource) {
      return new JdbcTemplate(dataSource);
    }

    /**
     * Scenarios by destination prefix: {@code lapse} (first send outlives its 2 s lease),
     * {@code acrm} (returns an id the hook records), {@code terminal} (401: the hook suspends),
     * {@code flaky} (fails once), {@code gate} / {@code gatefail} (wait for the test's gate,
     * then succeed / fail), {@code probe} (reads the pool).
     */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    OutboxPublisher leasePublisher(JdbcTemplate jdbc, DataSource dataSource) {
      return new OutboxPublisher() {
        @Override
        public boolean supports(OutboxEvent event) {
          return event.getDestination().matches("^[a-z]+-(ordered|concurrent):.*");
        }

        @Override
        public Lane lane(OutboxEvent event) {
          return event.getDestination().contains("-concurrent:") ? Lane.CONCURRENT : Lane.ORDERED;
        }

        @Override
        public Duration lease(OutboxEvent event) {
          return scenario(event).equals("lapse") ? Duration.ofSeconds(2) : null;
        }

        @Override
        public Duration backoff(OutboxEvent event, int attempt) {
          return Duration.ofMillis(200);
        }

        @Override
        public void publish(OutboxEvent event) {
          deliver(event);
        }

        @Override
        public Object deliver(OutboxEvent event) {
          long id = event.getId();
          SENT_IN_TX.add(TransactionSynchronizationManager.isActualTransactionActive());
          List<String> mine = DELIVERED.computeIfAbsent(id, k -> new CopyOnWriteArrayList<>());
          mine.add(OutboxHeaders.idempotencyKey("outbox-lease-it", id));
          switch (scenario(event)) {
            case "lapse" -> {
              if (mine.size() == 1) {
                latency(5000); // outlives the 2 s lease: the row is re-claimed meanwhile
                LATE_ANSWERED.countDown();
              }
            }
            case "terminal" -> throw new TerminalOutboxException("401 - subscription revoked");
            case "flaky" -> {
              if (mine.size() == 1) {
                throw new IllegalStateException("503 - try later");
              }
            }
            case "gate", "gatefail" -> {
              GATE_REACHED.countDown();
              awaitGate(GATE);
              if (scenario(event).equals("gatefail")) {
                throw new IllegalStateException("503 after the cancel");
              }
            }
            case "probe" -> ACTIVE_AT_SEND.put(id, active(dataSource));
            default -> {
              // acrm: delivered
            }
          }
          return "acrm-" + id;
        }

        @Override
        public void onBooked(OutboxEvent event, OutboxBooking booking) {
          assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isTrue();
          HOOKED
              .computeIfAbsent(event.getId(), k -> new CopyOnWriteArrayList<>())
              .add(booking.outcome());
          if (booking.outcome() == Outcome.RELAYED && scenario(event).equals("acrm")) {
            jdbc.update(
                "insert into side_effect (outbox_id, acrm_id) values (?, ?)",
                event.getId(),
                booking.result());
          }
          if (booking.outcome() == Outcome.EXHAUSTED) {
            jdbc.update(
                "insert into suspended (outbox_id, reason) values (?, ?)",
                event.getId(),
                booking.failure().getMessage());
          }
        }
      };
    }

    /** Refuses each acrm row's FIRST relayed booking, after the hook wrote its side effect. */
    @Bean
    OutboxRelayedListener refusingOnce() {
      return event -> {
        assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isTrue();
        LISTENED.computeIfAbsent(event.getId(), k -> new AtomicInteger()).incrementAndGet();
        if (scenario(event).equals("acrm") && REFUSED_ONCE.add(event.getId())) {
          throw new IllegalStateException("bookkeeping refused once");
        }
      };
    }
  }

  static String scenario(OutboxEvent event) {
    return event.getDestination().substring(0, event.getDestination().indexOf('-'));
  }

  static int active(DataSource dataSource) {
    try {
      return dataSource
          .unwrap(HikariDataSource.class)
          .getHikariPoolMXBean()
          .getActiveConnections();
    } catch (SQLException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** The receiver's response time: a timed wait on a latch nobody opens. */
  static void latency(long millis) {
    try {
      assertThat(new CountDownLatch(1).await(millis, TimeUnit.MILLISECONDS)).isFalse();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  static void awaitGate(CountDownLatch latch) {
    try {
      assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  @Autowired private OutboxWriter writer;
  @Autowired private OutboxMaintenanceService maintenance;
  @Autowired private TransactionTemplate tx;
  @Autowired private JdbcTemplate jdbc;
  @Autowired private MeterRegistry registry;

  private OutboxEvent append(String destination) {
    return tx.execute(s -> writer.append("lease", "a", "lease.v1", destination, Map.of()));
  }

  private OutboxRowView awaitRelayed(OutboxEvent row) {
    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(maintenance.inspect(row.getId()).relayedOn()).isNotNull());
    return maintenance.inspect(row.getId());
  }

  @BeforeEach
  void quiet() {
    await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> registry.get("opentmf.outbox.in-flight").gauge().value() == 0d);
  }

  @Test
  void aLapsedLease_isReClaimed_andTheLateHolderBooksNothing() {
    OutboxEvent row = append("lapse-concurrent:x");

    OutboxRowView booked = awaitRelayed(row); // by the SECOND holder, while the first still sends
    assertThat(DELIVERED.get(row.getId())).hasSize(2);
    awaitGate(LATE_ANSWERED); // the first holder has its answer and tries to book...
    await() // ...and books nothing: the row stays exactly as the second holder booked it
        .during(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(3))
        .untilAsserted(
            () ->
                assertThat(maintenance.inspect(row.getId()))
                    .returns(booked.relayedOn(), OutboxRowView::relayedOn)
                    .returns(0, OutboxRowView::attempts));
    assertThat(LISTENED.get(row.getId())).hasValue(1); // one booking, one listener call
    assertThat(HOOKED.get(row.getId())).containsExactly(Outcome.RELAYED);
    // both deliveries carry ONE idempotency key - the receiver dedups the second
    assertThat(DELIVERED.get(row.getId())).containsOnly("outbox-lease-it:outbox:" + row.getId());
  }

  /**
   * The booking hook's write commits with relayed_on and rolls back with it - on BOTH lanes: the
   * listener refuses the first booking AFTER the hook inserted its side effect; had that insert
   * survived, the second booking's insert would hit the primary key and never relay.
   */
  @Test
  void theBookingHook_commitsWithTheRelayedStamp_andRollsBackWithIt_onBothLanes() {
    for (String destination : List.of("acrm-ordered:x", "acrm-concurrent:x")) {
      OutboxEvent row = append(destination);

      OutboxRowView view = awaitRelayed(row);

      assertThat(view.attempts()).as(destination).isEqualTo(1); // the refused booking = a failure
      assertThat(view.lastError()).contains("bookkeeping refused once");
      assertThat(
              jdbc.queryForObject(
                  "select acrm_id from side_effect where outbox_id = ?",
                  String.class,
                  row.getId()))
          .isEqualTo("acrm-" + row.getId());
      assertThat(HOOKED.get(row.getId()))
          .containsExactly(Outcome.RELAYED, Outcome.RETRY, Outcome.RELAYED);
      assertThat(LISTENED.get(row.getId())).hasValue(2);
    }
    assertThat(SENT_IN_TX).containsOnly(false); // no send ever ran inside a transaction
  }

  /** 681's case: a terminal answer's DB write (the suspension) commits with the park. */
  @Test
  void theBookingHook_seesEveryOutcome_aTerminalOnesWriteCommitsWithThePark() {
    OutboxEvent terminal = append("terminal-concurrent:x");
    OutboxEvent flaky = append("flaky-ordered:x");

    await()
        .atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(maintenance.inspect(terminal.getId()).parked()).isTrue());
    awaitRelayed(flaky);

    assertThat(
            jdbc.queryForObject(
                "select reason from suspended where outbox_id = ?", String.class, terminal.getId()))
        .contains("401");
    assertThat(HOOKED.get(terminal.getId())).containsExactly(Outcome.EXHAUSTED);
    assertThat(HOOKED.get(flaky.getId())).containsExactly(Outcome.RETRY, Outcome.RELAYED);
  }

  /**
   * Cancel against a live lease: it does not wait for the send (no lock is held across it), and
   * the booking honours it - a delivered row is booked SENT-BUT-CANCELLED (listed under both
   * relayed and cancelled), a failed one retires cancelled, never retried, never parked.
   */
  @Test
  void aCancelDuringASend_returnsAtOnce_andTheBookingHonoursIt() throws Exception {
    OutboxEvent delivered = append("gate-concurrent:x");
    OutboxEvent failed = append("gatefail-concurrent:x");
    assertThat(GATE_REACHED.await(20, TimeUnit.SECONDS)).isTrue();
    assertThat(maintenance.inspect(delivered.getId()).inFlight()).isTrue();

    CompletableFuture<Void> cancels =
        CompletableFuture.runAsync(
            () -> {
              maintenance.cancel(delivered.getId());
              maintenance.cancel(failed.getId());
            });
    cancels.get(5, TimeUnit.SECONDS); // returns while both sends still wait at the gate
    GATE.countDown();

    OutboxRowView both = awaitRelayed(delivered);
    assertThat(both.cancelledOn()).isNotNull();
    assertThat(both.inFlight()).isFalse();
    assertThat(maintenance.list(null, OutboxStateFilter.RELAYED, PageRequest.of(0, 500)))
        .extracting(OutboxRowView::id)
        .contains(delivered.getId());
    assertThat(maintenance.list(null, OutboxStateFilter.CANCELLED, PageRequest.of(0, 500)))
        .extracting(OutboxRowView::id)
        .contains(delivered.getId(), failed.getId());

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> assertThat(HOOKED.get(failed.getId())).containsExactly(Outcome.CANCELLED));
    OutboxRowView retired = maintenance.inspect(failed.getId());
    assertThat(retired.relayedOn()).isNull();
    assertThat(retired.parked()).isFalse();
    assertThat(retired.claimedUntil()).isNull();
    assertThat(retired.attempts()).isEqualTo(1);
    await() // a sweep or two: a cancelled row is never claimed again
        .during(Duration.ofMillis(1500))
        .atMost(Duration.ofSeconds(3))
        .until(() -> DELIVERED.get(failed.getId()).size() == 1);
  }

  /**
   * The evidence for moving the ORDERED lane to the lease too: while an ORDERED send is in
   * flight on the relay thread, the pool has nothing checked out - and the same for CONCURRENT.
   */
  @Test
  void noConnectionIsCheckedOut_whileASendIsInFlight_onEitherLane() {
    OutboxEvent ordered = append("probe-ordered:x");
    awaitRelayed(ordered);
    OutboxEvent concurrent = append("probe-concurrent:x");
    awaitRelayed(concurrent);

    assertThat(ACTIVE_AT_SEND)
        .containsEntry(ordered.getId(), 0)
        .containsEntry(concurrent.getId(), 0);
  }

  /** Pre-1.3.0 rows and rows booked since carry no lease. */
  @Test
  void aBookedRow_carriesNoLease() {
    OutboxEvent row = append("acrm-ordered:y");
    OutboxRowView view = awaitRelayed(row);
    assertThat(view.claimedUntil()).isNull();
    assertThat(view.createdOn()).isBefore(OffsetDateTime.now().plusSeconds(1));
  }
}
