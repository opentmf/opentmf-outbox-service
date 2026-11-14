package org.opentmf.outboxshutdown;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opentmf.outbox.OutboxWriter;
import org.opentmf.outboxshutdown.ShutdownApp.RecordingPool;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * OUTBOX-SHUTDOWN-THREADS-BEFORE-DATASOURCE-1: a consumer's context closed while the library's
 * threads are busy - a send blocked on a dead broker (both lanes), the gauges' query in flight, the
 * heartbeat's renewal in flight - and in each case NO connection of the application's pool is held
 * or borrowed once the pool's close begins, NO thread of the library outlives the close, and the
 * test's jdbc:tc database stops with the context (Testcontainers stops it when its last connection
 * closes). Up to 1.11.0 the relay stopped in its {@code @PreDestroy} without waiting for its
 * threads: the blocked send woke on the interrupt and booked its failure, the refresher's and the
 * heartbeat's work went on, each borrowing from a pool already closing - and a test's jdbc:tc
 * container stayed up.
 */
class OutboxShutdownIT {

  private static ConfigurableApplicationContext start(String jdbcUrl, String... more) {
    ShutdownApp.sending = new CountDownLatch(1);
    String[] base = {
      "spring.main.web-application-type=none",
      "spring.application.name=outbox-shutdown-it",
      "it.jdbc-url=" + jdbcUrl,
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=200ms",
      "opentmf.outbox.metrics-refresh=500ms",
      "opentmf.outbox.lease=3s",
      "opentmf.outbox.send-timeout=2s",
      "opentmf.outbox.shutdown-grace=500ms",
      "opentmf.outbox.shutdown-join=3s"
    };
    String[] props = new String[base.length + more.length];
    System.arraycopy(base, 0, props, 0, base.length);
    System.arraycopy(more, 0, props, base.length, more.length);
    return new SpringApplicationBuilder(ShutdownApp.class).properties(props).run();
  }

  private static String url() {
    return "jdbc:tc:postgresql:18.1-alpine3.22:///shutdown"
        + UUID.randomUUID().toString().substring(0, 8);
  }

  private static void appendStuck(ConfigurableApplicationContext context) {
    OutboxWriter writer = context.getBean(OutboxWriter.class);
    new TransactionTemplate(context.getBean(PlatformTransactionManager.class))
        .executeWithoutResult(
            s -> writer.append("thing", "agg-1", "thing.v1", "stuck:t", Map.of()));
  }

  private static String postmasterStart(String jdbcUrl) throws Exception {
    try (Connection c = DriverManager.getConnection(jdbcUrl);
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("select pg_postmaster_start_time()::text")) {
      rs.next();
      return rs.getString(1);
    }
  }

  private static Set<Thread> libraryThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(Thread::isAlive)
        .filter(t -> t.getName().startsWith("opentmf-outbox-"))
        .collect(Collectors.toSet());
  }

  /** The close, and everything a clean stop must leave behind. */
  private static void closeAndCheck(
      ConfigurableApplicationContext context, String jdbcUrl, Set<Thread> before) throws Exception {
    RecordingPool pool = context.getBean(RecordingPool.class);
    String startedAt = postmasterStart(jdbcUrl); // a connection of the test's own, closed again

    context.close();

    assertThat(pool.heldAtClose).as("connections held when the pool's close began").isZero();
    assertThat(pool.borrowedAfterClose).as("borrowed after the pool's close began").isEmpty();
    Set<Thread> outliving = libraryThreads();
    outliving.removeAll(before);
    assertThat(outliving).as("library threads alive after the close").isEmpty();
    assertThat(postmasterStart(jdbcUrl))
        .as("the jdbc:tc database stopped with the context (a new one answers)")
        .isNotEqualTo(startedAt);
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {"CONCURRENT", "ORDERED"})
  void aSendBlockedOnADeadBroker_neitherHoldsNorBorrowsAtTheClose(String lane) throws Exception {
    Set<Thread> before = libraryThreads();
    String jdbcUrl = url();
    ConfigurableApplicationContext context = start(jdbcUrl, "it.lane=" + lane);
    appendStuck(context);
    assertThat(ShutdownApp.sending.await(30, TimeUnit.SECONDS)).as("the send under way").isTrue();

    closeAndCheck(context, jdbcUrl, before);
  }

  @Test
  void theGaugesQueryInFlight_endsBeforeThePoolCloses() throws Exception {
    Set<Thread> before = libraryThreads();
    String jdbcUrl = url();
    ConfigurableApplicationContext context = start(jdbcUrl);
    RecordingPool pool = context.getBean(RecordingPool.class);
    pool.slowFor = "opentmf-outbox-metrics";
    assertThat(pool.slowEntered.await(10, TimeUnit.SECONDS)).as("a refresh mid-way").isTrue();

    closeAndCheck(context, jdbcUrl, before);
  }

  @Test
  void theHeartbeatsRenewalInFlight_endsBeforeThePoolCloses() throws Exception {
    Set<Thread> before = libraryThreads();
    String jdbcUrl = url();
    ConfigurableApplicationContext context = start(jdbcUrl, "it.lane=CONCURRENT");
    RecordingPool pool = context.getBean(RecordingPool.class);
    appendStuck(context);
    assertThat(ShutdownApp.sending.await(30, TimeUnit.SECONDS)).isTrue();
    pool.slowFor = "opentmf-outbox-heartbeat"; // its next renewal of the send's lease, mid-way
    await().atMost(Duration.ofSeconds(10)).until(() -> pool.slowEntered.getCount() == 0);

    closeAndCheck(context, jdbcUrl, before);
  }
}
