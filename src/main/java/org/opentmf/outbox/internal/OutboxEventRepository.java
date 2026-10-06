package org.opentmf.outbox.internal;

import jakarta.persistence.LockModeType;
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
   * One window of the claim scan (1.3.0): {@code select … for update} over pending, not
   * cancelled, not parked, released (no hold, or the hold has passed) rows that are EITHER
   * claimable (due, and no live lease) OR in flight (a live lease) - in {@code id} order, past
   * {@code after}. This is the ONE place the eligibility predicate lives. The in-flight rows ride
   * along so the claim sees which CONCURRENT ordering keys are taken; it never claims them.
   *
   * <p>The lock WAITS (no SKIP LOCKED): every holder of these row locks - a claim, a booking,
   * an ops action - is a short transaction now, and waiting is what lets the claim see a row
   * another relay is stamping at that instant (PostgreSQL re-checks the predicate after the
   * wait, so a row just leased elsewhere is seen as in flight, never claimed twice), which keeps
   * an ordering key from going into flight twice across pods.
   */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query(
      """
      select e from OutboxEvent e
      where e.relayedOn is null and e.cancelledOn is null and e.parkedOn is null
        and (e.releaseAt is null or e.releaseAt <= :now)
        and e.id > :after
        and (e.claimedUntil > :now
          or (e.nextAttemptOn <= :now and (e.claimedUntil is null or e.claimedUntil <= :now)))
      order by e.id""")
  List<OutboxEvent> claimWindow(
      @Param("now") OffsetDateTime now, @Param("after") long after, Limit limit);

  /**
   * The LEASE GUARD: the row under a waiting {@code for update} lock, but only while it still
   * carries the lease this holder stamped - empty means the lease lapsed and another holder
   * re-stamped it (or it was booked), and the caller books nothing.
   */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("select e from OutboxEvent e where e.id = :id and e.claimedUntil = :stamp")
  Optional<OutboxEvent> lockLeased(@Param("id") long id, @Param("stamp") OffsetDateTime stamp);

  /** Pending rows under a live lease - backs the {@code in-flight} gauge. */
  @Query(
      """
      select count(e) from OutboxEvent e
      where e.relayedOn is null and e.cancelledOn is null and e.claimedUntil > :now""")
  long countInFlight(@Param("now") OffsetDateTime now);

  /**
   * One row under a WAITING {@code for update} lock (no SKIP LOCKED, no timeout hint) - the
   * ops actions (cancel, unpark) read through this so they serialize against a relay claim in
   * flight: the action sees the row AS THE RELAY LEFT IT, never a stale pre-claim snapshot.
   * Since 1.3.0 the relay holds no lock across a send - the action serialises against the short
   * claim and booking transactions only; a row in flight is cancellable (see the booking).
   */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("select e from OutboxEvent e where e.id = :id")
  Optional<OutboxEvent> lockById(@Param("id") long id);

  /** Pending rows (derived state: not relayed, not cancelled) — backs the {@code pending} gauge. */
  long countByRelayedOnIsNullAndCancelledOnIsNull();

  /** Parked rows (pending AND {@code parked_on} stamped) — backs the {@code parked} gauge. */
  long countByRelayedOnIsNullAndCancelledOnIsNullAndParkedOnIsNotNull();

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
