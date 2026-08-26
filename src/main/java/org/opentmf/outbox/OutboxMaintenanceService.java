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
 * The housekeeping + read surface behind the /ops trio (prune / unpark / list): a scheduled
 * job (e.g. a CronJob hitting the prune endpoint) triggers {@link #pruneRelayed()};
 * {@link #unpark(long)} is the explicit break-glass; {@link #list} and {@link #inspect} are the
 * no-direct-DB read half.
 */
@Slf4j
@RequiredArgsConstructor
public class OutboxMaintenanceService {

  private final OutboxEventRepository repository;
  private final OutboxProperties properties;
  private final ApplicationEventPublisher eventPublisher;

  /**
   * Prunes relayed rows older than {@code opentmf.outbox.retention} (default 7 days). Parked
   * rows are structurally never pruned ({@code relayed_on is null}).
   *
   * @return the number of rows deleted
   */
  @Transactional
  public long pruneRelayed() {
    OffsetDateTime cutoff = OffsetDateTime.now(ZoneOffset.UTC).minus(properties.getRetention());
    long pruned = repository.deleteByRelayedOnBefore(cutoff);
    if (pruned > 0) {
      log.info("Pruned {} relayed outbox rows older than {}", pruned, cutoff);
    }
    return pruned;
  }

  /**
   * Unparks one parked row after the root cause is fixed: resets {@code attempts}, makes the
   * row due now and nudges the relay (after this transaction commits). {@code last_error} is
   * kept for forensics until the row relays.
   *
   * @throws IllegalArgumentException when no row has the given id
   * @throws IllegalStateException when the row is not parked (already relayed, or retrying)
   */
  @Transactional
  public void unpark(long outboxId) {
    OutboxEvent event =
        repository
            .findById(outboxId)
            .orElseThrow(
                () -> new IllegalArgumentException("Outbox row %d not found".formatted(outboxId)));
    if (event.getRelayedOn() != null) {
      throw new IllegalStateException("Outbox row %d is already relayed".formatted(outboxId));
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
        case PENDING -> composed.and(row.relayedOn.isNull());
        case PARKED ->
            composed.and(row.relayedOn.isNull()).and(row.attempts.goe(properties.getMaxAttempts()));
        case RELAYED -> composed.and(row.relayedOn.isNotNull());
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
