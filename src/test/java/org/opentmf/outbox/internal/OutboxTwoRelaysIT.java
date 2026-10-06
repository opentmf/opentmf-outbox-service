package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxRelayedListener;
import org.opentmf.outbox.OutboxTestApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.data.domain.Limit;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The cross-pod guard on the real engine, now the LEASE: two claimers over the same rows never
 * take one row twice - the second WAITS for the first's short claim and then sees its rows as in
 * flight; and two whole relay instances (two workers, two lanes, one table) racing over ORDERED
 * and CONCURRENT rows deliver and book every row exactly once. The context's own relay is
 * parked (sweep 1h, rows seeded through the EntityManager so no after-commit nudge fires).
 */
// explicit classes (this IT sits in the internal package) switch off nested-config detection
@Import(OutboxTwoRelaysIT.Publishers.class)
@SpringBootTest(
    classes = OutboxTestApplication.class,
    properties = {
      "spring.application.name=outbox-two-relays-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///tworelays",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=1h",
      "opentmf.outbox.batch-size=5" // small claims, so the two pods interleave
    })
class OutboxTwoRelaysIT {

  static final Map<Long, AtomicInteger> DELIVERED = new ConcurrentHashMap<>();
  static final Map<Long, AtomicInteger> BOOKED = new ConcurrentHashMap<>();

  /** The threads ORDERED rows were sent on - one per "pod" driver. */
  static final Set<String> ORDERED_SENDERS = ConcurrentHashMap.newKeySet();

  @TestConfiguration
  static class Publishers {

    /** "conc:" rows ride the CONCURRENT lane, "ord:" rows the ORDERED one; each send ~20 ms. */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    OutboxPublisher racePublisher() {
      return new OutboxPublisher() {
        @Override
        public boolean supports(OutboxEvent event) {
          return event.getDestination().startsWith("conc:")
              || event.getDestination().startsWith("ord:");
        }

        @Override
        public Lane lane(OutboxEvent event) {
          return event.getDestination().startsWith("conc:") ? Lane.CONCURRENT : Lane.ORDERED;
        }

        @Override
        public void publish(OutboxEvent event) {
          DELIVERED.computeIfAbsent(event.getId(), id -> new AtomicInteger()).incrementAndGet();
          if (event.getDestination().startsWith("ord:")) {
            ORDERED_SENDERS.add(Thread.currentThread().getName());
          }
          try { // the receiver's response time: a timed wait on a latch nobody opens
            assertThat(new CountDownLatch(1).await(20, TimeUnit.MILLISECONDS)).isFalse();
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
      };
    }

    @Bean
    OutboxRelayedListener bookingCounter() {
      return event ->
          BOOKED.computeIfAbsent(event.getId(), id -> new AtomicInteger()).incrementAndGet();
    }
  }

  @Autowired private OutboxEventRepository repository;
  @Autowired private EntityManager entityManager;
  @Autowired private PlatformTransactionManager txManager;
  @Autowired private OutboxRelayWorker worker;
  @Autowired private OutboxPublisherRouter router;
  @Autowired private OutboxBackoff backoff;
  @Autowired private OutboxMetrics metrics;
  @Autowired private OutboxProperties properties;
  @Autowired private List<OutboxRelayedListener> listeners;

  private List<Long> seed(String destination, int count) {
    return new TransactionTemplate(txManager)
        .execute(
            status -> {
              List<Long> ids = new ArrayList<>();
              for (int i = 0; i < count; i++) {
                OutboxEvent row = new OutboxEvent();
                row.setAggregateType("t");
                row.setAggregateId("a-" + i);
                row.setEventType("e.v1");
                row.setDestination(destination);
                row.setPayload("{}");
                row.setCreatedOn(OffsetDateTime.now());
                row.setNextAttemptOn(OffsetDateTime.now().minusSeconds(1));
                entityManager.persist(row); // seal-safe seeding: the public entity, no nudge
                ids.add(row.getId());
              }
              return ids;
            });
  }

  @Test
  void aSecondClaimer_waitsForTheFirst_thenSeesItsRowsInFlight_neverTakesThemTwice()
      throws Exception {
    List<Long> ids = seed("never-relayed-" + System.nanoTime(), 2);
    TransactionTemplate tx = new TransactionTemplate(txManager);
    CountDownLatch firstHoldsItsRows = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    OffsetDateTime lease = OffsetDateTime.now().plusMinutes(5);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<?> first =
          pool.submit(
              () ->
                  tx.executeWithoutResult(
                      status -> {
                        List<OutboxEvent> mine = window(ids);
                        mine.forEach(row -> row.setClaimedUntil(lease)); // stamp, then hold
                        firstHoldsItsRows.countDown();
                        awaitLatch(release);
                      }));
      assertThat(firstHoldsItsRows.await(20, TimeUnit.SECONDS)).isTrue();
      Future<List<OutboxEvent>> second = pool.submit(() -> tx.execute(status -> window(ids)));
      await() // WAITING behind the first claim, not skipping past it
          .during(Duration.ofMillis(500))
          .atMost(Duration.ofSeconds(2))
          .until(() -> !second.isDone());
      release.countDown();
      first.get(20, TimeUnit.SECONDS);

      List<OutboxEvent> seen = second.get(20, TimeUnit.SECONDS);
      // the predicate was re-checked after the wait: both rows are there, as IN FLIGHT
      assertThat(seen)
          .hasSize(2)
          .allSatisfy(row -> assertThat(row.getClaimedUntil()).isAfter(OffsetDateTime.now()));
    } finally {
      pool.shutdownNow();
    }
  }

  private List<OutboxEvent> window(List<Long> ids) {
    return repository.claimWindow(OffsetDateTime.now(), ids.get(0) - 1, Limit.of(ids.size()));
  }

  @Test
  void twoRelayInstances_deliverAndBookEveryRowExactlyOnce() throws Exception {
    List<Long> rows = new ArrayList<>(seed("conc:race", 30));
    rows.addAll(seed("ord:race", 30));
    OutboxConcurrentLane secondLane =
        new OutboxConcurrentLane(4, false, Duration.ofSeconds(5));
    OutboxRelayWorker secondPod =
        new OutboxRelayWorker(
            repository,
            router,
            backoff,
            metrics,
            properties,
            listeners,
            new TransactionTemplate(txManager),
            secondLane);
    ExecutorService pods = Executors.newFixedThreadPool(2);
    try {
      long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
      Runnable drainFirst = () -> drain(worker, rows, deadline);
      Runnable drainSecond = () -> drain(secondPod, rows, deadline);
      Future<?> a = pods.submit(drainFirst);
      Future<?> b = pods.submit(drainSecond);
      a.get(90, TimeUnit.SECONDS);
      b.get(90, TimeUnit.SECONDS);
    } finally {
      pods.shutdownNow();
      secondLane.close();
    }

    // the pods really raced: ORDERED rows went out from more than one relay thread (the
    // context's own relay may join in too - a freed lane slot nudges it)
    assertThat(ORDERED_SENDERS).hasSizeGreaterThanOrEqualTo(2);
    assertThat(rows)
        .allSatisfy(
            id -> {
              assertThat(DELIVERED.get(id)).as("deliveries of row %d", id).hasValue(1);
              assertThat(BOOKED.get(id)).as("bookings of row %d", id).hasValue(1);
              assertThat(repository.findById(id).orElseThrow().getRelayedOn()).isNotNull();
            });
  }

  private void drain(OutboxRelayWorker relay, List<Long> rows, long deadline) {
    while (System.nanoTime() < deadline && !rows.stream().allMatch(BOOKED::containsKey)) {
      relay.relayBatch();
      Thread.onSpinWait();
    }
  }

  private static void awaitLatch(CountDownLatch latch) {
    try {
      assertThat(latch.await(Duration.ofSeconds(20).toMillis(), TimeUnit.MILLISECONDS)).isTrue();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(ex);
    }
  }
}
