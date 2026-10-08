package org.opentmf.outbox;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Predicate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opentmf.outbox.internal.OutboxAppended;
import org.opentmf.outbox.internal.OutboxEventRepository;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;

/**
 * The housekeeping + read surface behind the /ops endpoints (prune / unpark / cancel / list): a
 * scheduled job (e.g. a CronJob hitting the prune endpoint) triggers {@link #prune()};
 * {@link #unpark(long)} is the explicit break-glass; {@link #cancel(long)} withdraws an
 * unreleased effect; {@link #list} and {@link #inspect} are the no-direct-DB read half.
 *
 * <p>The row-mutating actions read their row under a WAITING {@code for update} lock, so they
 * serialize against the relay's short claim and booking transactions: an action sees the row as
 * the relay LEFT it (and refuses a now-relayed row) - never a stale snapshot, never a silent
 * no-op. Since 1.3.0 no lock is held across a send; since 1.4.0 a cancel REFUSES a row that is
 * IN FLIGHT (a live lease) with {@link OutboxRowInFlightException} (409) - its publisher may be
 * delivering right now - and succeeds on an unclaimed row or one whose lease has lapsed. The
 * booking still honours a cancel that slipped in on a lapsed lease (a late sender): a failed
 * send retires cancelled, a successful one is booked SENT-BUT-CANCELLED (both stamps). An unpark
 * never meets a live lease: only a booking parks a row, and the booking clears the lease.
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxMaintenanceService {

  /** The open ends of an unpark range: before any row was parked, after any will be. */
  private static final OffsetDateTime OPEN_FROM = OffsetDateTime.parse("1970-01-01T00:00:00Z");

  private static final OffsetDateTime OPEN_TO = OffsetDateTime.parse("9999-12-31T00:00:00Z");

  private final OutboxEventRepository repository;
  private final OutboxProperties properties;
  private final ApplicationEventPublisher eventPublisher;

  /**
   * Prunes the TERMINAL rows older than {@code opentmf.outbox.retention} (default 7 days):
   * relayed rows by {@code relayed_on}, cancelled rows by {@code cancelled_on} - one retention
   * for both, cancelled rows being kept that long for audit. Parked (and held) rows are
   * structurally never pruned: both timestamps are null.
   *
   * <p>Set-based and BOUNDED (1.3.0): batches of {@code opentmf.outbox.maintenance.batch-size}
   * rows, each its own short transaction, deleted by id without loading a single entity, oldest
   * first, until none are left or {@code opentmf.outbox.maintenance.time-budget} is spent - then
   * the result says more remains and the caller calls again. Deliberately NOT one transaction:
   * up to 1.2.1 the prune loaded every expired row as an entity in one transaction and, on a
   * multi-million row table, never finished.
   */
  public OutboxPruneResult pruneExpired() {
    OffsetDateTime cutoff = OffsetDateTime.now(ZoneOffset.UTC).minus(properties.getRetention());
    long deadline = System.nanoTime() + properties.getMaintenance().getTimeBudget().toNanos();
    int batch = properties.getMaintenance().getBatchSize();
    long relayed = 0;
    long cancelled = 0;
    boolean relayedLeft = true;
    boolean cancelledLeft = true;
    while ((relayedLeft || cancelledLeft) && System.nanoTime() < deadline) {
      if (relayedLeft) {
        int deleted = repository.deleteRelayedBatch(cutoff, batch);
        relayed += deleted;
        relayedLeft = deleted == batch;
      } else {
        int deleted = repository.deleteCancelledBatch(cutoff, batch);
        cancelled += deleted;
        cancelledLeft = deleted == batch;
      }
    }
    OutboxPruneResult result =
        new OutboxPruneResult(relayed, cancelled, relayedLeft || cancelledLeft);
    if (result.pruned() > 0 || result.moreToPrune()) {
      log.info(
          "Pruned {} relayed and {} cancelled outbox rows older than {}{}",
          relayed,
          cancelled,
          cutoff,
          result.moreToPrune() ? " - more remain, the time budget is spent" : "");
    }
    return result;
  }

  /**
   * One bounded prune call ({@link #pruneExpired()}), answering only the count; since 1.3.0 a
   * call may leave expired rows for the next one.
   *
   * @return the number of rows deleted by this call
   */
  public long prune() {
    return pruneExpired().pruned();
  }

  /**
   * The 1.0.0 name of {@link #prune()}, kept for source compatibility; since 1.1.0 it prunes
   * cancelled rows too (there is one retention for terminal rows).
   */
  public long pruneRelayed() {
    return pruneExpired().pruned();
  }

  /**
   * Cancels one UNRELEASED effect: the row is stamped {@code cancelled_on}, becomes
   * unclaimable for good, and is retained for audit until the retention prunes it. Only a row
   * that is not yet relayed, not already cancelled and not in flight is cancellable - the guard
   * is by name:
   *
   * @throws IllegalArgumentException when no row has the given id
   * @throws OutboxRowInFlightException when a relay holds a live lease on the row - its send may
   *     be delivering now (1.4.0); cancel again once it is booked
   * @throws IllegalStateException when the row is already relayed (the effect has left - a
   *     cancel cannot recall it) or already cancelled
   */
  @Transactional
  public void cancel(long outboxId) {
    OutboxEvent event = lock(outboxId);
    if (event.getRelayedOn() != null) {
      throw new IllegalStateException("Outbox row %d is already relayed".formatted(outboxId));
    }
    if (event.getCancelledOn() != null) {
      throw new IllegalStateException("Outbox row %d is already cancelled".formatted(outboxId));
    }
    OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
    if (event.getClaimedUntil() != null && event.getClaimedUntil().isAfter(now)) {
      // a LIVE lease: the publisher may be delivering right now - "cancelled" would be a lie
      throw new OutboxRowInFlightException(outboxId, event.getClaimedUntil());
    }
    event.setCancelledOn(now);
    log.info("Outbox row {} cancelled - it will never be relayed", outboxId);
  }

  /**
   * Unparks one parked row after the root cause is fixed: clears {@code parked_on}, resets
   * {@code attempts}, makes the row due now and nudges the relay (after this transaction
   * commits). {@code last_error} is kept for forensics until the row relays.
   *
   * @throws IllegalArgumentException when no row has the given id
   * @throws IllegalStateException when the row is not parked (already relayed, cancelled, or
   *     retrying)
   */
  @Transactional
  public void unpark(long outboxId) {
    OutboxEvent event = lock(outboxId);
    if (event.getRelayedOn() != null) {
      throw new IllegalStateException("Outbox row %d is already relayed".formatted(outboxId));
    }
    if (event.getCancelledOn() != null) {
      throw new IllegalStateException("Outbox row %d is cancelled".formatted(outboxId));
    }
    if (event.getParkedOn() == null) {
      throw new IllegalStateException(
          "Outbox row %d is not parked (attempts=%d, still retrying)"
              .formatted(outboxId, event.getAttempts()));
    }
    event.setParkedOn(null);
    event.setAttempts(0);
    event.setNextAttemptOn(OffsetDateTime.now(ZoneOffset.UTC));
    eventPublisher.publishEvent(new OutboxAppended(outboxId));
    log.info("Outbox row {} unparked - delivery will be retried", outboxId);
  }

  /**
   * Unparks by FILTER - the bulk form of {@link #unpark(long)}, for the morning after a
   * receiver's outage that outlasted its publisher's budget: every parked row of ONE destination
   * whose {@code parked_on} lies within {@code [parkedFrom, parkedTo)}, optionally of one
   * {@code reference}. Each row ends exactly as the single unpark leaves it ({@code parked_on}
   * cleared, {@code attempts} reset, due now, {@code last_error} kept), under the same guard: a
   * row an ops action or a booking holds is waited for and re-checked. The relay is nudged once.
   *
   * <p>Set-based and BOUNDED like {@link #pruneExpired()}: batches of
   * {@code opentmf.outbox.maintenance.batch-size}, each its own short transaction, oldest parked
   * first, until none match or {@code opentmf.outbox.maintenance.time-budget} is spent - then
   * the result says more remain and the caller calls again. What the relay then SENDS stays
   * bounded by the lanes: at most {@code concurrent.max-in-flight} CONCURRENT sends, at most
   * {@code batch-size} ORDERED rows per pass. A parked row is never leased (only a booking parks
   * a row, and that booking clears the lease), so no unparked row carries a live lease.
   *
   * @param destination REQUIRED - there is deliberately no "unpark everything"
   * @param parkedFrom inclusive lower bound of {@code parked_on}, or {@code null} for none
   * @param parkedTo exclusive upper bound of {@code parked_on}, or {@code null} for none
   * @param reference only rows of this private correlation, or {@code null} for any
   * @throws IllegalArgumentException when {@code destination} is missing or blank
   */
  public OutboxUnparkResult unpark(
      String destination, OffsetDateTime parkedFrom, OffsetDateTime parkedTo, String reference) {
    if (destination == null || destination.isBlank()) {
      throw new IllegalArgumentException(
          "An unpark by filter needs a destination - there is no unpark of everything");
    }
    // open ends as far bounds: one plain range in the SQL, never an OR on a parameter
    OffsetDateTime from = parkedFrom != null ? parkedFrom : OPEN_FROM;
    OffsetDateTime to = parkedTo != null ? parkedTo : OPEN_TO;
    long deadline = System.nanoTime() + properties.getMaintenance().getTimeBudget().toNanos();
    int batch = properties.getMaintenance().getBatchSize();
    long unparked = 0;
    boolean left = true;
    while (left && System.nanoTime() < deadline) {
      OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
      int done =
          reference == null
              ? repository.unparkBatch(now, destination, from, to, batch)
              : repository.unparkBatchByReference(now, destination, from, to, reference, batch);
      unparked += done;
      left = done == batch;
    }
    if (unparked > 0) {
      eventPublisher.publishEvent(new OutboxAppended(0)); // one nudge for the whole call
    }
    if (unparked > 0 || left) {
      log.info(
          "Outbox unpark by filter: {} row(s) to {} unparked{}",
          unparked,
          destination,
          left ? " - more remain, the time budget is spent" : "");
    }
    return new OutboxUnparkResult(unparked, left);
  }

  private OutboxEvent lock(long outboxId) {
    return repository
        .lockById(outboxId)
        .orElseThrow(
            () -> new IllegalArgumentException("Outbox row %d not found".formatted(outboxId)));
  }

  /**
   * The TMF630 triage list: the caller's Querydsl predicate is
   * AND-composed with the CLOSED derived-state filter ({@link OutboxStateFilter} - the state
   * legs are null-tests over several columns, hence the dedicated parameter). Payloads are
   * omitted in lists; {@link #inspect(long)} carries them.
   */
  @Transactional(readOnly = true)
  public Page<OutboxRowView> list(Predicate predicate, OutboxStateFilter state, Pageable pageable) {
    QOutboxEvent row = QOutboxEvent.outboxEvent;
    BooleanBuilder composed = new BooleanBuilder();
    if (predicate != null) {
      composed.and(predicate);
    }
    if (state != null) {
      switch (state) {
        case PENDING -> composed.and(row.relayedOn.isNull()).and(row.cancelledOn.isNull());
        case PARKED -> composed.and(row.parkedOn.isNotNull()).and(row.cancelledOn.isNull());
        case RELAYED -> composed.and(row.relayedOn.isNotNull());
        case CANCELLED -> composed.and(row.cancelledOn.isNotNull());
      }
    }
    return repository
        .findAll(composed, pageable)
        .map(event -> OutboxRowView.of(event, false));
  }

  /**
   * One row in full, payload and {@code last_error} included - the forensic read that precedes
   * an {@link #unpark(long)} decision.
   *
   * @throws IllegalArgumentException when no row has the given id
   */
  @Transactional(readOnly = true)
  public OutboxRowView inspect(long outboxId) {
    return repository
        .findById(outboxId)
        .map(event -> OutboxRowView.of(event, true))
        .orElseThrow(
            () -> new IllegalArgumentException("Outbox row %d not found".formatted(outboxId)));
  }
}
