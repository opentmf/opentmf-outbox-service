package org.opentmf.outbox.internal;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.opentmf.outbox.OutboxArchRules;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxMaintenanceService;
import org.opentmf.outbox.OutboxWriter;
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
 * {@link QuerydslPredicateExecutor} backs the TMF630 ops list.
 */
public interface OutboxEventRepository
    extends JpaRepository<OutboxEvent, Long>, QuerydslPredicateExecutor<OutboxEvent> {

  /**
   * The claim query: {@code select … for update skip locked} over pending, not cancelled,
   * released (no hold, or the hold has passed), due, non-parked rows in {@code id} order. This
   * is the ONE place the eligibility predicate lives. The pessimistic lock plus SKIP LOCKED
   * (Hibernate lock timeout {@code -2}) is the cross-pod guard; within a pod the relay is
   * single-threaded.
   */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
  @Query(
      """
      select e from OutboxEvent e
      where e.relayedOn is null and e.cancelledOn is null
        and (e.releaseAt is null or e.releaseAt <= :now)
        and e.nextAttemptOn <= :now and e.attempts < :maxAttempts
      order by e.id""")
  List<OutboxEvent> claimBatch(
      @Param("now") OffsetDateTime now, @Param("maxAttempts") int maxAttempts, Limit limit);

  /**
   * One row under a WAITING {@code for update} lock (no SKIP LOCKED, no timeout hint) - the
   * ops actions (cancel, unpark) read through this so they serialize against a relay claim in
   * flight: the action sees the row AS THE RELAY LEFT IT, never a stale pre-claim snapshot.
   */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("select e from OutboxEvent e where e.id = :id")
  Optional<OutboxEvent> lockById(@Param("id") long id);

  /** Pending rows (derived state: not relayed, not cancelled) — backs the {@code pending} gauge. */
  long countByRelayedOnIsNullAndCancelledOnIsNull();

  /** Parked rows (pending AND attempts exhausted) — backs the {@code parked} gauge. */
  long countByRelayedOnIsNullAndCancelledOnIsNullAndAttemptsGreaterThanEqual(int attempts);

  /**
   * The instant the oldest RELEASED pending row became deliverable — {@code created_on}, or
   * the hold if that came later — backs the {@code relay-lag} gauge. Held (future
   * {@code release_at}) and cancelled rows are not lagging and do not count.
   */
  @Query(
      """
      select min(case when e.releaseAt > e.createdOn then e.releaseAt else e.createdOn end)
      from OutboxEvent e
      where e.relayedOn is null and e.cancelledOn is null
        and (e.releaseAt is null or e.releaseAt <= :now)""")
  Optional<OffsetDateTime> findOldestPendingSince(@Param("now") OffsetDateTime now);

  /**
   * Retention pruning: deletes rows relayed before the cutoff. Parked rows have
   * {@code relayed_on is null}, so they structurally never match — parked rows are NEVER
   * pruned automatically.
   */
  long deleteByRelayedOnBefore(OffsetDateTime cutoff);

  /** Retention pruning of the other terminal state: cancelled rows past the cutoff. */
  long deleteByCancelledOnBefore(OffsetDateTime cutoff);
}
