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
 * serialize against a relay claim in flight: an action that races the relay sees the row as
 * the relay LEFT it (and refuses a now-relayed row) - never a stale snapshot, never a silent
 * no-op.
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxMaintenanceService {

  private final OutboxEventRepository repository;
  private final OutboxProperties properties;
  private final ApplicationEventPublisher eventPublisher;

  /**
   * Prunes the TERMINAL rows older than {@code opentmf.outbox.retention} (default 7 days):
   * relayed rows by {@code relayed_on}, cancelled rows by {@code cancelled_on} - one retention
   * for both, cancelled rows being kept that long for audit. Parked (and held) rows are
   * structurally never pruned: both timestamps are null.
   *
   * @return the number of rows deleted
   */
  @Transactional
  public long prune() {
    return doPrune();
  }

  /**
   * The 1.0.0 name of {@link #prune()}, kept for source compatibility; since 1.1.0 it prunes
   * cancelled rows too (there is one retention for terminal rows).
   */
  @Transactional
  public long pruneRelayed() {
    return doPrune();
  }

  /** Un-annotated on purpose: both public names delegate here, never to each other. */
  private long doPrune() {
    OffsetDateTime cutoff = OffsetDateTime.now(ZoneOffset.UTC).minus(properties.getRetention());
    long relayed = repository.deleteByRelayedOnBefore(cutoff);
    long cancelled = repository.deleteByCancelledOnBefore(cutoff);
    if (relayed + cancelled > 0) {
      log.info(
          "Pruned {} relayed and {} cancelled outbox rows older than {}",
          relayed,
          cancelled,
          cutoff);
    }
    return relayed + cancelled;
  }

  /**
   * Cancels one UNRELEASED effect: the row is stamped {@code cancelled_on}, becomes
   * unclaimable for good, and is retained for audit until the retention prunes it. Only a row
   * that is not yet relayed and not already cancelled is cancellable - the guard is by name:
   *
   * @throws IllegalArgumentException when no row has the given id
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
    event.setCancelledOn(OffsetDateTime.now(ZoneOffset.UTC));
    log.info("Outbox row {} cancelled - it will never be relayed", outboxId);
  }

  /**
   * Unparks one parked row after the root cause is fixed: resets {@code attempts}, makes the
   * row due now and nudges the relay (after this transaction commits). {@code last_error} is
   * kept for forensics until the row relays.
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
    if (event.getAttempts() < properties.getMaxAttempts()) {
      throw new IllegalStateException(
          "Outbox row %d is not parked (attempts=%d < max-attempts=%d)"
              .formatted(outboxId, event.getAttempts(), properties.getMaxAttempts()));
    }
    event.setAttempts(0);
    event.setNextAttemptOn(OffsetDateTime.now(ZoneOffset.UTC));
    eventPublisher.publishEvent(new OutboxAppended(outboxId));
    log.info("Outbox row {} unparked - delivery will be retried", outboxId);
  }

  private OutboxEvent lock(long outboxId) {
    return repository
        .lockById(outboxId)
        .orElseThrow(
            () -> new IllegalArgumentException("Outbox row %d not found".formatted(outboxId)));
  }

  /**
   * The TMF630 triage list: the caller's Querydsl predicate is
   * AND-composed with the CLOSED derived-state filter ({@link OutboxStateFilter} - a config
   * comparison Querydsl cannot express from the wire, hence the dedicated parameter). Payloads
   * are omitted in lists; {@link #inspect(long)} carries them.
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
        case PARKED ->
            composed
                .and(row.relayedOn.isNull())
                .and(row.cancelledOn.isNull())
                .and(row.attempts.goe(properties.getMaxAttempts()));
        case RELAYED -> composed.and(row.relayedOn.isNotNull());
        case CANCELLED -> composed.and(row.cancelledOn.isNotNull());
      }
    }
    return repository
        .findAll(composed, pageable)
        .map(event -> OutboxRowView.of(event, properties.getMaxAttempts(), false));
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
        .map(event -> OutboxRowView.of(event, properties.getMaxAttempts(), true))
        .orElseThrow(
            () -> new IllegalArgumentException("Outbox row %d not found".formatted(outboxId)));
  }
}
