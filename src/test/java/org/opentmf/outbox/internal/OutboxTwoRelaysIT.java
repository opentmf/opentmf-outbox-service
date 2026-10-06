package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.springframework.data.domain.Limit;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The cross-pod guard on the real engine: two claimers get disjoint rows ({@code SKIP LOCKED},
 * never blocking); a pod claiming a key's first row keeps every other pod off that key; and two
 * whole relay instances (two workers, two lanes, one table) racing over ORDERED and CONCURRENT
 * rows deliver and book every row exactly once. The context's own relay is parked (sweep 1h,
 * rows seeded through the EntityManager so no after-commit nudge fires).
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

  /** Seeds claimable rows - stamped with a lane and key as the writer would - with no nudge. */
  private List<Long> seed(String destination, int count, Lane lane, String key) {
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
                row.setLane(lane);
                row.setOrderingKey(key);
                entityManager.persist(row); // seal-safe seeding: the public entity, no nudge
                ids.add(row.getId());
              }
              return ids;
            });
  }

  private List<Long> seed(String destination, int count) {
    Lane lane = destination.startsWith("conc:") ? Lane.CONCURRENT : Lane.ORDERED;
    return seed(destination, count, lane, null);
  }

  /**
   * The cross-pod guard (H.4, restored for 1.3.0): two concurrent claimers over the same
   * claimable rows get DISJOINT rows - {@code FOR UPDATE SKIP LOCKED} skips what the other holds
   * rather than blocking or double-claiming.
   */
  @Test
  void twoConcurrentClaimers_getDisjointRows_skipLockedNeverBlocksNorDoubleClaims()
      throws Exception {
    List<Long> seeded = seed("never-relayed-" + System.nanoTime(), 2, Lane.ORDERED, null);
    TransactionTemplate tx = new TransactionTemplate(txManager);
    CountDownLatch firstHoldsItsRow = new CountDownLatch(1);
    CountDownLatch secondIsDone = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<Long> first =
          pool.submit(
              () ->
                  tx.execute(
                      status -> {
                        Long mine = oneOrderedRow(); // locks exactly one row
                        firstHoldsItsRow.countDown();
                        awaitLatch(secondIsDone); // keep the lock while the other claims
                        return mine;
                      }));
      Future<Long> second =
          pool.submit(
              () -> {
                awaitLatch(firstHoldsItsRow);
                try {
                  return tx.execute(status -> oneOrderedRow());
                } finally {
                  secondIsDone.countDown();
                }
              });

      Long secondId = second.get(20, TimeUnit.SECONDS); // would hang forever if it BLOCKED
      Long firstId = first.get(20, TimeUnit.SECONDS);
      assertThat(List.of(firstId, secondId)).containsExactlyInAnyOrderElementsOf(seeded);
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * The ordering key across pods, the normal case: pod A holds the claim of r1 (key K) - its
   * stamp not committed yet - and pod B claims at that moment. B skips the locked r1, and r2 is
   * not claimable for B either: in B's committed view r1 is still due, a LOWER row of K. B takes
   * nothing of K - without ever seeing A's uncommitted stamp.
   */
  @Test
  void whileOnePodClaimsAKeysFirstRow_anotherPodTakesNothingOfThatKey() throws Exception {
    String key = "receiver-" + System.nanoTime();
    List<Long> ids = seed("never-relayed-" + key, 2, Lane.CONCURRENT, key);
    TransactionTemplate tx = new TransactionTemplate(txManager);
    CountDownLatch podAHoldsR1 = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<List<Long>> podA =
          pool.submit(
              () ->
                  tx.execute(
                      status -> {
                        List<OutboxEvent> mine = concurrentOf(key);
                        OffsetDateTime lease = OffsetDateTime.now().plusMinutes(5);
                        mine.forEach(row -> row.setClaimedUntil(lease));
                        podAHoldsR1.countDown();
                        awaitLatch(release);
                        return mine.stream().map(OutboxEvent::getId).toList();
                      }));
      awaitLatch(podAHoldsR1);
      List<Long> podB =
          pool.submit(
                  () ->
                      tx.execute(
                          status ->
                              concurrentOf(key).stream().map(OutboxEvent::getId).toList()))
              .get(20, TimeUnit.SECONDS); // never blocks
      release.countDown();

      assertThat(podA.get(20, TimeUnit.SECONDS)).containsExactly(ids.get(0)); // one per key
      assertThat(podB).isEmpty();
    } finally {
      pool.shutdownNow();
    }
  }

  private Long oneOrderedRow() {
    return repository.claimOrdered(OffsetDateTime.now(), Lane.CONCURRENT, Limit.of(1)).stream()
        .map(OutboxEvent::getId)
        .findFirst()
        .orElse(null);
  }

  private List<OutboxEvent> concurrentOf(String key) {
    return repository
        .claimConcurrent(OffsetDateTime.now(), "", 500)
        .stream()
        .filter(row -> key.equals(row.getOrderingKey()))
        .toList();
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
