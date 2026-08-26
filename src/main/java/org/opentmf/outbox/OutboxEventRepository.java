package org.opentmf.outbox;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.query.Param;

/**
 * Repository for {@link OutboxEvent}. Accessed only by the library itself (writer, relay,
 * metrics, maintenance) — consumers go through {@link OutboxWriter} and
 * {@link OutboxMaintenanceService}; the seal is enforceable via {@link OutboxArchRules}.
 * {@link QuerydslPredicateExecutor} backs the TMF630 ops list (2026-08-26 toolkit amendment).
 */
interface OutboxEventRepository
    extends JpaRepository<OutboxEvent, Long>, QuerydslPredicateExecutor<OutboxEvent> {

  /**
   * The §23 claim query: {@code select … for update skip locked} over pending, due, non-parked
   * rows in {@code id} order. The pessimistic lock plus SKIP LOCKED (Hibernate lock timeout
   * {@code -2}) is the cross-pod guard; within a pod the relay is single-threaded.
   */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
  @Query(
      """
      select e from OutboxEvent e
      where e.relayedOn is null and e.nextAttemptOn <= :now and e.attempts < :maxAttempts
      order by e.id""")
  List<OutboxEvent> claimBatch(
      @Param("now") OffsetDateTime now, @Param("maxAttempts") int maxAttempts, Limit limit);

  /** Pending rows (§23 derived state) — backs the {@code pending} gauge. */
  long countByRelayedOnIsNull();

  /** Parked rows (pending AND attempts exhausted) — backs the {@code parked} gauge. */
  long countByRelayedOnIsNullAndAttemptsGreaterThanEqual(int attempts);

  /** Creation time of the oldest pending row — backs the {@code relay-lag} gauge. */
  @Query("select min(e.createdOn) from OutboxEvent e where e.relayedOn is null")
  Optional<OffsetDateTime> findOldestPendingCreatedOn();

  /**
   * Retention pruning: deletes rows relayed before the cutoff. Parked rows have
   * {@code relayed_on is null}, so they structurally never match — §23: parked rows are NEVER
   * pruned automatically.
   */
  long deleteByRelayedOnBefore(OffsetDateTime cutoff);
}
