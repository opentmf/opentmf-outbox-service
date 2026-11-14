package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxHeaders;
import org.opentmf.outbox.OutboxMaintenanceService;
import org.opentmf.outbox.OutboxTestApplication;
import org.opentmf.outbox.OutboxWriter;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext.HierarchyMode;
import org.springframework.test.context.TestContextManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * OUTBOX-RELAY-RESTARTS-AFTER-A-STOP-1: the relay and the gauges' refresher are {@code
 * SmartLifecycle} beans since 1.11.1, and a Lifecycle bean that was stopped is started again by
 * whoever stopped it - {@code ConfigurableApplicationContext.stop()/start()}, and Spring Framework
 * 7's test-context cache, which PAUSES a cached context when a test class switches to another one
 * and restarts it when a later class reuses it ({@code spring.test.context.cache.pause}, default
 * {@code on_context_switch}). A relay stopped once must relay again once started: a booked row is
 * delivered and the gauges refresh again.
 *
 * <p>The switch is Spring's own: each step drives a {@link TestContextManager} for a holder class
 * as the JUnit extension does - load (or reuse) the context, then {@code afterTestClass}, which
 * marks it unused - so the pause and the restart are the context cache's, not this test's.
 */
class OutboxRelayRestartIT {

  private static final String COMMON_DB =
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver";
  private static final String CHANGELOG =
      "spring.liquibase.change-log=classpath:db/test-changelog.xml";
  private static final String SWEEP = "opentmf.outbox.sweep-interval=1s";
  private static final String REFRESH = "opentmf.outbox.metrics-refresh=500ms";

  /** The idempotency keys the receiver has answered 200 to. */
  private static final Set<String> RECEIVED = ConcurrentHashMap.newKeySet();

  private static HttpServer receiver;
  private static String destination;

  @SpringBootTest(
      classes = OutboxTestApplication.class,
      properties = {
        "spring.application.name=outbox-restart-first",
        "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///restartfirst",
        COMMON_DB,
        CHANGELOG,
        SWEEP,
        REFRESH
      })
  static class First {}

  @SpringBootTest(
      classes = OutboxTestApplication.class,
      properties = {
        "spring.application.name=outbox-restart-second",
        "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///restartsecond",
        COMMON_DB,
        CHANGELOG,
        SWEEP,
        REFRESH
      })
  static class Second {}

  @SpringBootTest(
      classes = OutboxTestApplication.class,
      properties = {
        "spring.application.name=outbox-restart-stop-start",
        "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///restartstopstart",
        COMMON_DB,
        CHANGELOG,
        SWEEP,
        REFRESH
      })
  static class StopStart {}

  @BeforeAll
  static void startReceiver() throws IOException {
    receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    receiver.createContext(
        "/receive",
        exchange -> {
          RECEIVED.add(exchange.getRequestHeaders().getFirst(OutboxHeaders.IDEMPOTENCY_KEY));
          exchange.getRequestBody().readAllBytes();
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });
    receiver.start();
    destination = "http://127.0.0.1:" + receiver.getAddress().getPort() + "/receive";
  }

  @AfterAll
  static void stopReceiver() {
    receiver.stop(0);
  }

  /** The Lifecycle contract as the context calls it: stop, start - and the relay relays again. */
  @Test
  void aStoppedContext_startedAgain_relaysAndRefreshesAgain() throws Exception {
    TestContextManager manager = new TestContextManager(StopStart.class);
    try {
      ConfigurableApplicationContext context =
          (ConfigurableApplicationContext) manager.getTestContext().getApplicationContext();
      assertDelivered(context, "before the stop");

      MeterRegistry registry = context.getBean(MeterRegistry.class);
      assertThat(registry.get(OutboxMetrics.RELAY_RUNNING).gauge().value()).isEqualTo(1d);
      context.stop();
      assertThat(context.getBean(OutboxRelay.class).isRunning()).isFalse();
      assertThat(registry.get(OutboxMetrics.RELAY_RUNNING).gauge().value()).isZero();
      context.start();

      assertThat(context.getBean(OutboxRelay.class).isRunning()).isTrue();
      assertThat(registry.get(OutboxMetrics.RELAY_RUNNING).gauge().value()).isEqualTo(1d);
      assertThat(context.getBean(OutboxMetrics.class).isRunning()).isTrue();
      assertDelivered(context, "after stop and start");
      assertRefreshing(context);
    } finally {
      manager.getTestContext().markApplicationContextDirty(HierarchyMode.EXHAUSTIVE);
      manager.afterTestClass();
    }
  }

  /**
   * Spring's test-context cache: First's context is paused when Second's starts and restarted when
   * a later class reuses it - and its relay relays again.
   */
  @Test
  void aCachedContext_pausedOnASwitch_relaysAgainWhenReused() throws Exception {
    TestContextManager first = new TestContextManager(First.class);
    TestContextManager second = new TestContextManager(Second.class);
    TestContextManager firstAgain = new TestContextManager(First.class);
    try {
      ApplicationContext context = first.getTestContext().getApplicationContext();
      assertDelivered(context, "on the first use");
      first.afterTestClass(); // the class is done: its context is unused, still cached

      second.getTestContext().getApplicationContext(); // the switch: First's context pauses
      assertThat(((ConfigurableApplicationContext) context).isRunning()).isFalse();
      second.afterTestClass();

      ApplicationContext reused = firstAgain.getTestContext().getApplicationContext();
      assertThat(reused).isSameAs(context); // the cache's hit, restarted
      assertThat(((ConfigurableApplicationContext) reused).isRunning()).isTrue();
      assertDelivered(reused, "after the switch back");
      assertRefreshing(reused);
      firstAgain.afterTestClass();
    } finally {
      first.getTestContext().markApplicationContextDirty(HierarchyMode.EXHAUSTIVE);
      second.getTestContext().markApplicationContextDirty(HierarchyMode.EXHAUSTIVE);
    }
  }

  /** A row booked now is delivered and booked RELAYED within the sweep's reach. */
  private static void assertDelivered(ApplicationContext context, String when) {
    OutboxWriter writer = context.getBean(OutboxWriter.class);
    TransactionTemplate tx =
        new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
    OutboxEvent row =
        tx.execute(s -> writer.append("restart", when, "restart.v1", destination, Map.of()));
    OutboxMaintenanceService maintenance = context.getBean(OutboxMaintenanceService.class);
    String key =
        OutboxHeaders.idempotencyKey(
            context.getEnvironment().getProperty("spring.application.name"), row);
    await(when)
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () -> {
              assertThat(key).as("delivered to the receiver").isIn(RECEIVED);
              assertThat(maintenance.inspect(row.getId()).relayedOn()).isNotNull();
            });
  }

  /**
   * The gauges refresh again: three seconds on - six refresh periods - metrics-age is under two (a
   * refresher that stays stopped has aged past three by then).
   */
  private static void assertRefreshing(ApplicationContext context) {
    MeterRegistry registry = context.getBean(MeterRegistry.class);
    await("the gauges refresh again")
        .pollDelay(Duration.ofSeconds(3))
        .atMost(Duration.ofSeconds(15))
        .untilAsserted(
            () ->
                assertThat(registry.get(OutboxMetrics.METRICS_AGE).gauge().value())
                    .isLessThan(2.0));
  }
}
