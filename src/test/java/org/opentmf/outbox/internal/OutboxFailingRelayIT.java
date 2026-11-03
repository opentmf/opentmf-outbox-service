package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.opentmf.outbox.OutboxRelayedListener;
import org.opentmf.outbox.OutboxTestApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The claim pause on the real engine (OUTBOX-FAILING-RELAY-CLAIMS-1, 1.11.0), found by a
 * consumer's two-pod IT beside a cached context whose producer pointed at a dead broker.
 *
 * <p>(1) A pod whose TRANSPORT is down - its broker unreachable from that pod, while another pod
 * reaches it - beside a healthy pod: every row it takes fails its attempt and waits out its
 * backoff, though the healthy pod would have sent it at once. Up to 1.10.0 it took 102 of 300 rows
 * (98 on 1.9.1's claim rule), 20 and 12 when each failure took a second; with the pause it takes
 * the threshold's worth and the sends already under way, then leaves the rows to the healthy pod.
 *
 * <p>(2) A receiver that is down for every pod - a dead HTTP subscriber - holds back no other
 * receiver's rows: its publisher names no transport, so nothing pauses. A lane-wide pause (the
 * first 1.11.0 draft) let 354 of 1,000 healthy rows through in a minute beside one dead
 * subscriber.
 *
 * <p>(3) A transport down for EVERY pod, its backlog ahead of another publisher's rows in the same
 * id-ordered stream: the rows behind are not starved - the claim reads a skip window past the
 * paused rows, and a paused row due longer than {@code claim-pause.max} is taken as before - and
 * a pass of skipped rows never makes the relay claim again at once.
 */
// explicit classes (this IT sits in the internal package) switch off nested-config detection
@Import(OutboxFailingRelayIT.Publishers.class)
@SpringBootTest(
    classes = OutboxTestApplication.class,
    properties = {
      "spring.application.name=outbox-failing-relay-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///failingrelay",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=1h",
      "opentmf.outbox.batch-size=20"
    })
@DirtiesContext
class OutboxFailingRelayIT {

  /** {@code claim-pause.after-failures}'s default: failed sends in a row before claims pause. */
  static final int PAUSE_AFTER = 5;

  /** What the failing pod may take over a run: the threshold, plus the sends under way. */
  static final int FAILED_ROWS_BOUND = PAUSE_AFTER + 5;

  static final String TRANSPORT = "test-broker";
  static final int KEYS = 100;
  static final int ROWS_PER_KEY = 3;

  static final Set<Long> DELIVERED = ConcurrentHashMap.newKeySet();
  static final Set<Long> BOOKED = ConcurrentHashMap.newKeySet();

  @TestConfiguration
  static class Publishers {

    /** The healthy pod's publisher: "live:" rows over {@link #TRANSPORT}, ~5 ms a send. */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    OutboxPublisher livePublisher() {
      return new LivePublisher();
    }

    @Bean
    OutboxRelayedListener bookingRecorder() {
      return event -> BOOKED.add(event.getId());
    }
  }

  /** CONCURRENT rows keyed by destination, over {@link #TRANSPORT}; every send succeeds. */
  static class LivePublisher implements OutboxPublisher {
    @Override
    public boolean supports(OutboxEvent event) {
      return event.getDestination().startsWith("live:");
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
    public String transport(OutboxEvent event) {
      return TRANSPORT;
    }

    @Override
    public void publish(OutboxEvent event) {
      pause(5);
      DELIVERED.add(event.getId());
    }
  }

  /** The failing pod's publisher: the same rows over the same transport - every send fails. */
  static final class DeadPublisher extends LivePublisher {
    final AtomicInteger sends = new AtomicInteger();
    final Set<Long> rows = ConcurrentHashMap.newKeySet();
    private final long failMillis;

    DeadPublisher(long failMillis) {
      this.failMillis = failMillis;
    }

    @Override
    public void publish(OutboxEvent event) {
      sends.incrementAndGet();
      rows.add(event.getId());
      pause(failMillis);
      throw new IllegalStateException("broker unreachable from this pod (test)");
    }
  }

  /**
   * Rows to {@code <prefix>/...} destinations, unkeyed CONCURRENT: those ending in "/dead" always
   * fail; a named transport only when {@code transport} is set.
   */
  static final class MixedPublisher implements OutboxPublisher {
    final Map<Long, Long> sentAt = new ConcurrentHashMap<>();
    private final String prefix;
    private final String transport;

    MixedPublisher(String prefix, String transport) {
      this.prefix = prefix;
      this.transport = transport;
    }

    @Override
    public boolean supports(OutboxEvent event) {
      return event.getDestination().startsWith(prefix);
    }

    @Override
    public Lane lane(OutboxEvent event) {
      return Lane.CONCURRENT;
    }

    @Override
    public String transport(OutboxEvent event) {
      return event.getDestination().endsWith("/dead") ? transport : null;
    }

    @Override
    public void publish(OutboxEvent event) {
      pause(5);
      if (event.getDestination().endsWith("/dead")) {
        throw new IllegalStateException("503 from the receiver (test)");
      }
      sentAt.put(event.getId(), System.nanoTime());
    }
  }

  @Autowired private EntityManager entityManager;
  @Autowired private PlatformTransactionManager txManager;
  @Autowired private OutboxEventRepository repository;
  @Autowired private OutboxRelayWorker worker;
  @Autowired private OutboxBackoff backoff;
  @Autowired private OutboxProperties properties;
  @Autowired private List<OutboxRelayedListener> listeners;

  /** The test pods' own meters: their claim pauses show here. */
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

  /** Each test on a table of its own rows: whatever an earlier one left open is cancelled. */
  @BeforeEach
  void cancelLeftovers() {
    new TransactionTemplate(txManager)
        .executeWithoutResult(
            status ->
                entityManager
                    .createNativeQuery(
                        "update outbox set cancelled_on = now()"
                            + " where coalesce(relayed_on, cancelled_on, parked_on) is null")
                    .executeUpdate());
  }

  /** A relay instance of its own - a second pod on the same table. */
  private record Pod(
      OutboxRelayWorker worker, OutboxConcurrentLane lane, OutboxOrderedLane orderedLane)
      implements AutoCloseable {
    @Override
    public void close() {
      lane.close();
      orderedLane.close();
    }
  }

  private Pod pod(OutboxPublisher publisher, OutboxProperties settings) {
    OutboxConcurrentLane lane =
        new OutboxConcurrentLane(8, settings.getBatchSize(), false, Duration.ofSeconds(5));
    OutboxOrderedLane orderedLane = new OutboxOrderedLane(Duration.ofSeconds(5));
    OutboxRelayWorker relay =
        new OutboxRelayWorker(
            repository,
            new OutboxPublisherRouter(List.of(publisher)),
            backoff,
            new OutboxMetrics(registry, repository, Duration.ofSeconds(15)),
            settings,
            listeners,
            new TransactionTemplate(txManager),
            new TransactionTemplate(txManager),
            lane,
            orderedLane,
            OutboxClock.system(),
            OutboxSendHeartbeatFixture.idle());
    return new Pod(relay, lane, orderedLane);
  }

  private static OutboxProperties settings(Duration pauseMax) {
    OutboxProperties settings = new OutboxProperties();
    settings.setBatchSize(20);
    settings.getClaimPause().setMax(pauseMax);
    return settings;
  }

  @ParameterizedTest(name = "a send fails after {0} ms")
  @ValueSource(longs = {0, 1000})
  void aPodWhoseTransportIsDown_soonLeavesItsRowsToTheHealthyPod(long failMillis)
      throws Exception {
    List<Long> rows = seed("live:" + failMillis + "-");
    DeadPublisher dead = new DeadPublisher(failMillis);
    AtomicBoolean done = new AtomicBoolean();
    ExecutorService pods = Executors.newFixedThreadPool(2);
    long started = System.nanoTime();
    try (Pod deadPod = pod(dead, properties)) {
      long deadline = started + Duration.ofSeconds(90).toNanos();
      Future<?> healthy = pods.submit(() -> drain(worker, rows, deadline, done));
      Future<?> failing = pods.submit(() -> drain(deadPod.worker(), rows, deadline, done));
      healthy.get(120, TimeUnit.SECONDS);
      done.set(true);
      failing.get(30, TimeUnit.SECONDS);
    } finally {
      done.set(true);
      pods.shutdownNow();
    }
    long millis = (System.nanoTime() - started) / 1_000_000;
    long failedRows = rows.stream().filter(id -> attempts(id) > 0).count();
    int mostAttempts = rows.stream().mapToInt(this::attempts).max().orElse(0);
    long unsent = rows.stream().filter(id -> !DELIVERED.contains(id)).count();
    System.out.printf(
        "FAILING-RELAY: fail after %d ms: %d rows, %d keys; the failing pod sent %d times over"
            + " %d rows; %d rows booked a failed attempt (at most %d on one row); %d rows"
            + " still unsent after %d ms (backoff-base %s)%n",
        failMillis,
        rows.size(),
        KEYS,
        dead.sends.get(),
        dead.rows.size(),
        failedRows,
        mostAttempts,
        unsent,
        millis,
        properties.getBackoffBase());

    assertThat(dead.rows)
        .as(
            "rows the failing pod took from the healthy one (it should pause the transport after"
                + " %d failed sends in a row)",
            PAUSE_AFTER)
        .hasSizeLessThanOrEqualTo(FAILED_ROWS_BOUND);
    assertThat(rows).as("every row relayed by the healthy pod").allMatch(DELIVERED::contains);
  }

  /**
   * One pod, a steady stream - every 20 ms a row to a healthy receiver and one to a receiver that
   * answers 503 every time: the healthy rows are not held back (the receiver names no transport).
   */
  @Test
  void aReceiverDownForEveryPod_holdsNoOtherReceiversRowsBack() {
    MixedPublisher mixed = new MixedPublisher("http-like/", null);
    List<Long> healthy = new ArrayList<>();
    Map<Long, Long> appendedAt = new ConcurrentHashMap<>();
    long started = System.nanoTime();
    long deadline = started + Duration.ofSeconds(60).toNanos();
    try (Pod pod = pod(mixed, settings(Duration.ofMinutes(1)))) {
      int appended = 0;
      while (System.nanoTime() < deadline
          && (appended < 1000 || !mixed.sentAt.keySet().containsAll(healthy))) {
        if (appended < 1000 && System.nanoTime() - started > appended * 20_000_000L) {
          appended++;
          long id =
              new TransactionTemplate(txManager)
                  .execute(
                      status -> {
                        row("http-like/dead", null);
                        return row("http-like/live", null).getId();
                      });
          healthy.add(id);
          appendedAt.put(id, System.nanoTime());
        }
        pod.worker().relayBatch(false);
        pause(2);
      }
    }
    List<Long> latencies =
        healthy.stream()
            .filter(mixed.sentAt::containsKey)
            .map(id -> (mixed.sentAt.get(id) - appendedAt.get(id)) / 1_000_000)
            .sorted()
            .toList();
    System.out.printf(
        "DEAD-RECEIVER: %d of %d healthy rows sent; latency p50 %d ms, max %d ms%n",
        latencies.size(),
        healthy.size(),
        latencies.isEmpty() ? -1 : latencies.get(latencies.size() / 2),
        latencies.isEmpty() ? -1 : latencies.get(latencies.size() - 1));

    assertThat(latencies).hasSize(1000);
  }

  /**
   * A transport down for EVERY pod (here the only one), its unkeyed backlog of 300 rows AHEAD of
   * another publisher's 30 in id order - far more than the skip window (8 slots x 10): the 30 are
   * relayed, none of the paused transport's rows is sent beside the probes and the unattended
   * ones, and no pass that leased nothing came back full.
   */
  @Test
  void aTransportDownForEveryPod_neitherStarvesTheRowsBehindItsBacklog_norLoopsTheClaim() {
    MixedPublisher mixed = new MixedPublisher("starve/", "starve-broker");
    OutboxProperties settings = settings(Duration.ofSeconds(3));
    List<Long> behind = new ArrayList<>();
    int emptyFullPasses = 0;
    int passes = 0;
    long started = System.nanoTime();
    try (Pod pod = pod(mixed, settings)) {
      new TransactionTemplate(txManager)
          .executeWithoutResult(
              status -> {
                for (int i = 0; i < 300; i++) {
                  row("starve/dead", null);
                }
              });
      // the transport pauses on the first rows it fails
      long pauseBy = System.nanoTime() + Duration.ofSeconds(20).toNanos();
      while (claimsPaused("starve-broker") != 1d && System.nanoTime() < pauseBy) {
        pod.worker().relayBatch(false);
        pause(5);
      }
      assertThat(claimsPaused("starve-broker")).as("the transport paused").isEqualTo(1d);
      new TransactionTemplate(txManager)
          .executeWithoutResult(
              status -> {
                for (int i = 0; i < 30; i++) {
                  behind.add(row("starve/live", null).getId());
                }
              });
      long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (System.nanoTime() < deadline && !mixed.sentAt.keySet().containsAll(behind)) {
        OutboxRelayWorker.Batch batch = pod.worker().relayBatch(false);
        passes++;
        if (batch.concurrent() == 0 && batch.concurrentFull()) {
          emptyFullPasses++;
        }
        pause(5);
      }
    }
    long millis = (System.nanoTime() - started) / 1_000_000;
    long sentBehind = behind.stream().filter(mixed.sentAt::containsKey).count();
    System.out.printf(
        "STARVATION: %d of %d rows behind a paused backlog of 300 sent in %d ms (claim-pause.max"
            + " %s); %d passes, %d of them leased nothing yet came back full%n",
        sentBehind,
        behind.size(),
        millis,
        settings.getClaimPause().getMax(),
        passes,
        emptyFullPasses);

    assertThat(behind).as("rows behind the paused backlog").allMatch(mixed.sentAt::containsKey);
    assertThat(emptyFullPasses).as("passes of skipped rows that claimed again at once").isZero();
  }

  /** The transport's {@code claims.paused} value on this JVM's pods; 0 before its first pause. */
  private double claimsPaused(String transport) {
    Gauge gauge =
        registry
            .find(OutboxMetrics.CLAIMS_PAUSED)
            .tag(OutboxMetrics.TAG_TRANSPORT, transport)
            .gauge();
    return gauge == null ? 0d : gauge.value();
  }

  /** {@link #KEYS} keys of {@link #ROWS_PER_KEY} rows each, due now, with no nudge. */
  private List<Long> seed(String prefix) {
    return new TransactionTemplate(txManager)
        .execute(
            status -> {
              List<Long> ids = new ArrayList<>();
              for (int r = 0; r < ROWS_PER_KEY; r++) {
                for (int k = 0; k < KEYS; k++) {
                  String key = prefix + "%03d".formatted(k);
                  ids.add(row(key, key).getId());
                }
              }
              return ids;
            });
  }

  /** One CONCURRENT row due now, seeded through the entity (seal-safe, no nudge). */
  private OutboxEvent row(String destination, String key) {
    OutboxEvent row = new OutboxEvent();
    row.setAggregateType("t");
    row.setAggregateId(key == null ? "a" : key);
    row.setEventType("e.v1");
    row.setDestination(destination);
    row.setPayload("{}");
    row.setCreatedOn(OffsetDateTime.now());
    row.setNextAttemptOn(OffsetDateTime.now().minusSeconds(1));
    row.setLane(Lane.CONCURRENT);
    row.setOrderingKey(key);
    entityManager.persist(row);
    return row;
  }

  private void drain(OutboxRelayWorker relay, List<Long> rows, long deadline, AtomicBoolean done) {
    while (!done.get()
        && System.nanoTime() < deadline
        && !rows.stream().allMatch(BOOKED::contains)) {
      relay.relayBatch(false);
      pause(2);
    }
  }

  private int attempts(long id) {
    return repository.findById(id).orElseThrow().getAttempts();
  }

  private static void pause(long millis) {
    try { // a timed wait on a latch nobody opens
      assertThat(new CountDownLatch(1).await(millis, TimeUnit.MILLISECONDS)).isFalse();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }
}
