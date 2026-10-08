package org.opentmf.outbox.internal;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.opentmf.outbox.OutboxArchRules;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxMaintenanceService;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.opentmf.outbox.OutboxWriter;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Repository for {@link OutboxEvent}. Accessed only by the library itself (writer, relay,
 * metrics, maintenance) — consumers go through {@link OutboxWriter} and
 * {@link OutboxMaintenanceService}; the seal is enforceable via {@link OutboxArchRules}.
 * {@link QuerydslPredicateExecutor} backs the TMF630 ops list.
 */
public interface OutboxEventRepository
    extends JpaRepository<OutboxEvent, Long>, QuerydslPredicateExecutor<OutboxEvent> {

  /** The query timeout hint (not a planner hint): a gauge query cannot hang the refresher. */
  String GAUGE_TIMEOUT_HINT = "jakarta.persistence.query.timeout";

  /** A gauge query that does not answer within 5 s fails the refresh (the gauges read NaN). */
  String GAUGE_TIMEOUT_MILLIS = "5000";


  /**
   * The ORDERED lane's claim: {@code select … for update skip locked} over pending, not
   * cancelled, not parked, released (no hold, or the hold has passed), due rows with no live
   * lease whose stamped lane is ORDERED or null (a row written before 1.3.0, or not through the
   * writer), in {@code id} order. Together with {@link #claimConcurrent} this is the ONE place
   * the eligibility predicate lives. {@code SKIP LOCKED} (Hibernate lock timeout {@code -2}) is
   * the cross-pod guard: a row another pod is claiming is skipped, and once that claim commits
   * the row carries a live lease.
   */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
  @Query(
      """
      select e from OutboxEvent e
      where e.relayedOn is null and e.cancelledOn is null and e.parkedOn is null
        and (e.releaseAt is null or e.releaseAt <= :now)
        and e.nextAttemptOn <= :now
        and (e.claimedUntil is null or e.claimedUntil <= :now)
        and (e.lane is null or e.lane <> :concurrent)
      order by e.id""")
  List<OutboxEvent> claimOrdered(
      @Param("now") OffsetDateTime now, @Param("concurrent") Lane concurrent, Limit limit);

  /**
   * The CONCURRENT lane's claim, first pass ({@link OutboxClaimSql#CONCURRENT}): the same
   * eligibility over rows stamped CONCURRENT, plus the ORDERING-KEY rule - a keyed row is
   * claimable only as its key's first due row and only while no row of its key is in flight, and
   * its rows go in {@code id} order on the happy path; a row in backoff lets later rows of its
   * key pass, as before. These rows are CANDIDATES: across pods the worker then takes each key's
   * advisory lock ({@link #tryLockKey}) and re-checks them in a new snapshot
   * ({@link #recheckKeyedHeads}), so a key never has two rows in flight, within a pod or across
   * pods.
   *
   * @param after the round-robin cursor: only keys above it ({@code ""} = from the first key)
   * @param limit rows to claim at most (the free lane slots)
   */
  @Query(value = OutboxClaimSql.CONCURRENT, nativeQuery = true)
  List<OutboxEvent> claimConcurrent(
      @Param("now") OffsetDateTime now, @Param("after") String after, @Param("limit") int limit);

  /**
   * The CONCURRENT lane's wrap-around pass ({@link OutboxClaimSql#CONCURRENT_WRAP}): keyed rows
   * only, keys from the first up to {@code until} (the cursor), by the same rules.
   */
  @Query(value = OutboxClaimSql.CONCURRENT_WRAP, nativeQuery = true)
  List<OutboxEvent> claimConcurrentWrap(
      @Param("now") OffsetDateTime now, @Param("until") String until, @Param("limit") int limit);

  /**
   * Takes - never waits for - the claim transaction's advisory lock of one ordering key
   * ({@link OutboxClaimSql#KEY_LOCK}).
   *
   * @return whether the lock was taken; {@code false} = another pod is claiming the key now
   */
  @Query(value = OutboxClaimSql.KEY_LOCK, nativeQuery = true)
  boolean tryLockKey(@Param("namespace") int namespace, @Param("key") String key);

  /**
   * Of the keyed candidates, those still claimable as their key's head with nothing of their key
   * in flight - in a NEW snapshot, after the key locks ({@link OutboxClaimSql#RECHECK}).
   *
   * @param ids the candidates as a PostgreSQL array literal, e.g. {@code {7,12}}
   */
  @Query(value = OutboxClaimSql.RECHECK, nativeQuery = true)
  List<Long> recheckKeyedHeads(@Param("now") OffsetDateTime now, @Param("ids") String ids);

  /**
   * The LEASE GUARD: the row under a waiting {@code for update} lock, but only while it still
   * carries the lease this holder stamped - empty means the lease lapsed and another holder
   * re-stamped it (or it was booked), and the caller books nothing.
   */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("select e from OutboxEvent e where e.id = :id and e.claimedUntil = :stamp")
  Optional<OutboxEvent> lockLeased(@Param("id") long id, @Param("stamp") OffsetDateTime stamp);

  /**
   * One row under a WAITING {@code for update} lock (no SKIP LOCKED, no timeout hint) - the
   * ops actions (cancel, unpark) read through this so they serialize against a relay claim in
   * flight: the action sees the row AS THE RELAY LEFT IT, never a stale pre-claim snapshot.
   * Since 1.3.0 the relay holds no lock across a send - the action serialises against the short
   * claim and booking transactions only; a row in flight is refused (1.4.0, see cancel). The
   * claims skip a row an ops action holds.
   */
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("select e from OutboxEvent e where e.id = :id")
  Optional<OutboxEvent> lockById(@Param("id") long id);

  /** Open rows that are not parked (pending minus parked) - half of the {@code pending} gauge. */
  @QueryHints(@QueryHint(name = GAUGE_TIMEOUT_HINT, value = GAUGE_TIMEOUT_MILLIS))
  @Query(value = OutboxGaugeSql.OPEN_NOT_PARKED, nativeQuery = true)
  long countOpenNotParked();

  /** Parked rows - the {@code parked} gauge, and the other half of {@code pending}. */
  @QueryHints(@QueryHint(name = GAUGE_TIMEOUT_HINT, value = GAUGE_TIMEOUT_MILLIS))
  @Query(value = OutboxGaugeSql.PARKED, nativeQuery = true)
  long countParked();

  /**
   * The instant the oldest open row became deliverable - {@code created_on}, or its hold if that
   * came later; a held row's instant lies in the future - backs the {@code relay-lag} gauge.
   */
  @QueryHints(@QueryHint(name = GAUGE_TIMEOUT_HINT, value = GAUGE_TIMEOUT_MILLIS))
  @Query(value = OutboxGaugeSql.OPEN_SINCE, nativeQuery = true)
  Optional<Instant> findOldestOpenSince(); // a native timestamptz scalar comes back as Instant

  /** Open rows under a live lease - the {@code in-flight} gauge. */
  @QueryHints(@QueryHint(name = GAUGE_TIMEOUT_HINT, value = GAUGE_TIMEOUT_MILLIS))
  @Query(value = OutboxGaugeSql.IN_FLIGHT, nativeQuery = true)
  long countInFlight(@Param("now") OffsetDateTime now);

  /**
   * Retention prune, one BATCH of relayed rows older than the cutoff, in its OWN transaction -
   * {@code REQUIRES_NEW}: a caller already in a transaction does not hold every batch in it.
   * Parked rows have {@code relayed_on is null}, so they structurally never match.
   *
   * @return the rows deleted - fewer than {@code limit} means none are left
   */
  @Modifying
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @Query(value = OutboxPruneSql.RELAYED, nativeQuery = true)
  int deleteRelayedBatch(@Param("cutoff") OffsetDateTime cutoff, @Param("limit") int limit);

  /** Retention prune of the other terminal state: one batch of cancelled rows. */
  @Modifying
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @Query(value = OutboxPruneSql.CANCELLED, nativeQuery = true)
  int deleteCancelledBatch(@Param("cutoff") OffsetDateTime cutoff, @Param("limit") int limit);

  /**
   * Unpark by filter, one BATCH: one destination's parked rows within the parked-on range, oldest
   * parked first, each left as the single unpark leaves it.
   *
   * @return the rows unparked - fewer than {@code limit} means none are left
   */
  @Modifying
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @Query(value = OutboxUnparkSql.BY_DESTINATION, nativeQuery = true)
  int unparkBatch(
      @Param("now") OffsetDateTime now,
      @Param("destination") String destination,
      @Param("parkedFrom") OffsetDateTime parkedFrom,
      @Param("parkedTo") OffsetDateTime parkedTo,
      @Param("limit") int limit);

  /** The same, narrowed to one {@code reference}. */
  @Modifying
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  @Query(value = OutboxUnparkSql.BY_REFERENCE, nativeQuery = true)
  int unparkBatchByReference(
      @Param("now") OffsetDateTime now,
      @Param("destination") String destination,
      @Param("parkedFrom") OffsetDateTime parkedFrom,
      @Param("parkedTo") OffsetDateTime parkedTo,
      @Param("reference") String reference,
      @Param("limit") int limit);
}
