package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * OUTBOX-CONCURRENT-CLAIM-INDEPENDENT-1 (1.8.0): the CONCURRENT lane never waits behind an ORDERED
 * batch. Up to 1.7.0 the relay thread sent its ORDERED batch itself, so the next CONCURRENT claim
 * waited until the batch's last row was sent - measured on a flow service: 26 s without a
 * CONCURRENT row while ordered HTTP legs went out at about one a second. Here 100 ORDERED rows to a
 * receiver answering in 150 ms (a 15 s batch) are sending when a CONCURRENT row is appended: it is
 * delivered within two seconds while the ORDERED batch is still going, and the ORDERED rows still
 * go one at a time, in {@code id} order.
 */
@SpringBootTest(
    properties = {
      "spring.application.name=outbox-lanes-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///lanes",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=1s",
      "opentmf.outbox.batch-size=100"
    })
class OutboxLaneIndependenceIT {

  static final List<Long> ORDERED_SENT = new CopyOnWriteArrayList<>();
  static final AtomicInteger ORDERED_NOW = new AtomicInteger();
  static final AtomicInteger ORDERED_MAX = new AtomicInteger();
  static final Map<Long, Integer> CONCURRENT_SENT = new ConcurrentHashMap<>();
  static final Map<Long, Long> CONCURRENT_SENT_AT = new ConcurrentHashMap<>();

  @TestConfiguration
  static class Receivers {

    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
      return new TransactionTemplate(txManager);
    }

    /** "ord:" rows answer in 150 ms on the ORDERED lane; "conc:" rows answer at once. */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    OutboxPublisher receivers() {
      return new OutboxPublisher() {
        @Override
        public boolean supports(OutboxEvent event) {
          return event.getDestination().matches("^(ord|conc):.*");
        }

        @Override
        public Lane lane(OutboxEvent event) {
          return event.getDestination().startsWith("conc:") ? Lane.CONCURRENT : Lane.ORDERED;
        }

        @Override
        public void publish(OutboxEvent event) {
          if (event.getDestination().startsWith("conc:")) {
            // how many ORDERED rows had gone out when this one did
            CONCURRENT_SENT_AT.put(event.getId(), System.nanoTime());
            CONCURRENT_SENT.put(event.getId(), ORDERED_SENT.size());
            return;
          }
          ORDERED_MAX.accumulateAndGet(ORDERED_NOW.incrementAndGet(), Math::max);
          try { // the slow receiver's response time
            assertThat(new CountDownLatch(1).await(150, TimeUnit.MILLISECONDS)).isFalse();
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          } finally {
            ORDERED_NOW.decrementAndGet();
          }
          ORDERED_SENT.add(event.getId());
        }
      };
    }
  }

  @Autowired private OutboxWriter writer;
  @Autowired private TransactionTemplate tx;

  @Test
  void aConcurrentRow_isNotHeldBehindAnOrderedBatch_andTheOrderedLaneKeepsItsOrder() {
    List<Long> ordered =
        tx.execute(
            status ->
                IntStream.range(0, 100)
                    .mapToObj(
                        i ->
                            writer
                                .append(
                                    "it-aggregate", "o-" + i, "it.ordered.v1", "ord:slow", Map.of())
                                .getId())
                    .toList());
    await().atMost(Duration.ofSeconds(10)).until(() -> ORDERED_SENT.size() >= 3); // sending now

    long appendedAt = System.nanoTime();
    long concurrent =
        tx.execute(
            status ->
                writer
                    .append("it-aggregate", "c-1", "it.concurrent.v1", "conc:fast", Map.of())
                    .getId());

    await().atMost(Duration.ofSeconds(2)).until(() -> CONCURRENT_SENT.containsKey(concurrent));
    System.out.printf(
        "LANE-INDEPENDENCE: the CONCURRENT row went out %d ms after its append, after %d of 100"
            + " ORDERED rows (150 ms each)%n",
        (CONCURRENT_SENT_AT.get(concurrent) - appendedAt) / 1_000_000,
        CONCURRENT_SENT.get(concurrent));
    assertThat(CONCURRENT_SENT.get(concurrent))
        .as("ORDERED rows sent before the CONCURRENT row - the batch was still going")
        .isLessThan(50);

    await().atMost(Duration.ofSeconds(40)).until(() -> ORDERED_SENT.size() == 100);
    List<Long> sent = List.copyOf(ORDERED_SENT);
    assertThat(sent).containsExactlyElementsOf(ordered); // id order
    assertThat(ORDERED_MAX.get()).isEqualTo(1); // one at a time
  }
}
