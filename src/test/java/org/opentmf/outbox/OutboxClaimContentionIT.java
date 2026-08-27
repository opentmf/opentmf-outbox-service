package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.internal.OutboxEventRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Limit;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The cross-pod guard on the real engine (H.4): two concurrent claimers over the same claimable
 * rows get DISJOINT rows - {@code FOR UPDATE SKIP LOCKED} skips what the other holds rather
 * than blocking or double-claiming. The relay is parked (sweep 1h, rows seeded through the
 * EntityManager so no after-commit nudge fires) so only the two test claimers compete.
 */
@SpringBootTest(
    properties = {
      "spring.application.name=outbox-contention-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///contention",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=1h"
    })
class OutboxClaimContentionIT {

  @Autowired private OutboxEventRepository repository;
  @Autowired private EntityManager entityManager;
  @Autowired private PlatformTransactionManager txManager;

  private void seedClaimableRows(int count) {
    new TransactionTemplate(txManager)
        .executeWithoutResult(
            status -> {
              for (int i = 0; i < count; i++) {
                OutboxEvent row = new OutboxEvent();
                row.setAggregateType("t");
                row.setAggregateId("a-" + i);
                row.setEventType("e.v1");
                row.setDestination("never-relayed");
                row.setPayload("{}");
                row.setCreatedOn(OffsetDateTime.now());
                row.setNextAttemptOn(OffsetDateTime.now().minusSeconds(1));
                entityManager.persist(row); // seal-safe seeding: the public entity, no nudge
              }
            });
  }

  @Test
  void twoConcurrentClaimers_getDisjointRows_skipLockedNeverBlocksNorDoubleClaims()
      throws Exception {
    seedClaimableRows(2);
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
                        List<OutboxEvent> mine =
                            repository.claimBatch(OffsetDateTime.now(), Limit.of(1));
                        firstHoldsItsRow.countDown();
                        await(secondIsDone); // keep the lock while the other claimer runs
                        return mine.get(0).getId();
                      }));
      Future<Long> second =
          pool.submit(
              () -> {
                await(firstHoldsItsRow);
                try {
                  return tx.execute(
                      status ->
                          repository.claimBatch(OffsetDateTime.now(), Limit.of(1)).get(0).getId());
                } finally {
                  secondIsDone.countDown();
                }
              });

      Long secondId = second.get(20, TimeUnit.SECONDS); // would hang forever if it BLOCKED
      Long firstId = first.get(20, TimeUnit.SECONDS);
      assertThat(secondId).isNotEqualTo(firstId); // skipped the held row, took the next
    } finally {
      pool.shutdownNow();
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertThat(latch.await(Duration.ofSeconds(20).toMillis(), TimeUnit.MILLISECONDS)).isTrue();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(ex);
    }
  }
}
