package org.opentmf.outbox.internal;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxPublisher.ExhaustionOutcome;
import org.opentmf.outbox.OutboxRelayedListener;
import org.opentmf.outbox.TerminalOutboxException;
import org.springframework.data.domain.Limit;
import org.springframework.transaction.annotation.Transactional;

/**
 * One relay pass: claims a batch of due pending rows ({@code FOR UPDATE SKIP LOCKED}, id
 * order), resolves each row's publisher FIRST, publishes and books the outcome by THAT
 * publisher's failure policy - all in ONE claim transaction. On success {@code relayed_on} is
 * set; a crash between delivery and commit means redelivery - that is the at-least-once
 * contract, and the {@code x-idempotency-key} makes consumer dedup trivial.
 *
 * <p>On failure: {@code attempts++}, {@code last_error} recorded, {@code next_attempt_on}
 * pushed out by the publisher's backoff (library exponential backoff by default); at the
 * publisher's budget (library {@code max-attempts} by default) the row is EXHAUSTED: PARK
 * stamps {@code parked_on} (excluded from claims, the {@code parked} gauge alerts, unparking
 * is an explicit ops action) or DROP stamps {@code relayed_on} with the forensics kept (no
 * listener fires, the {@code dropped} counter books it). A {@link TerminalOutboxException}
 * reaches exhaustion immediately. A failed row does not stop the batch. The failure
 * bookkeeping NEVER touches {@code release_at}: the scheduled-send hold and the retry schedule
 * are separate facts (the entity maps the hold {@code updatable = false}, and a regression
 * test names the property).
 */
@Slf4j
@RequiredArgsConstructor
class OutboxRelayWorker {

  /** Bound for the persisted {@code last_error} text - keeps forensic rows sane. */
  static final int LAST_ERROR_MAX_LENGTH = 4000;

  private final OutboxEventRepository repository;
  private final OutboxPublisherRouter router;
  private final OutboxBackoff backoff;
  private final OutboxMetrics metrics;
  private final OutboxProperties properties;
  private final List<OutboxRelayedListener> relayedListeners;

  /**
   * Claims and relays one batch.
   *
   * <p>The whole batch runs in ONE transaction, which is safe against the per-row invariant
   * (a later row's failure must never roll back an earlier row's already-stood publish)
   * because {@link #relayOne} catches every {@link RuntimeException} and BOOKS it - no
   * per-row failure ever propagates to the transaction. The residual trade is a SYSTEMIC
   * failure at commit (the database gone at that instant): every row in the batch is then
   * redelivered on the next pass - correct under at-least-once plus the idempotency key,
   * just a blast radius of up to {@code batch-size} rows instead of one, on a rare event.
   *
   * @return the number of rows claimed - the relay drains while this equals the batch size
   */
  @Transactional
  public int relayBatch() {
    List<OutboxEvent> claimed =
        repository.claimBatch(
            OffsetDateTime.now(ZoneOffset.UTC), Limit.of(properties.getBatchSize()));
    claimed.forEach(this::relayOne);
    return claimed.size();
  }

  private void relayOne(OutboxEvent event) {
    OutboxPublisher publisher;
    try {
      publisher = router.resolve(event);
    } catch (IllegalStateException ex) {
      registerFailure(event, null, ex); // unroutable: the LIBRARY policy books it
      return;
    }
    try {
      publisher.publish(event);
    } catch (TerminalOutboxException ex) {
      bookAttempt(event, ex);
      exhaust(event, publisher);
      return;
    } catch (RuntimeException ex) {
      registerFailure(event, publisher, ex);
      return;
    }
    event.setRelayedOn(OffsetDateTime.now(ZoneOffset.UTC));
    try {
      // the post-relay seam (OutboxRelayedListener): consumer bookkeeping stamped in the SAME
      // claim transaction, with relayedOn already set. A throwing listener falls through to
      // the ordinary failure path - which must then UNDO the stamp, or the row would commit
      // as relayed-with-attempts++ nonsense.
      for (OutboxRelayedListener listener : relayedListeners) {
        listener.onRelayed(event);
      }
      metrics.recordRelayed(event.getDestination(), event.getAttempts() + 1);
    } catch (RuntimeException ex) {
      event.setRelayedOn(null); // a listener failed AFTER the stamp - the row retries
      registerFailure(event, publisher, ex);
    }
  }

  private void registerFailure(OutboxEvent event, OutboxPublisher publisher, RuntimeException ex) {
    int attempts = bookAttempt(event, ex);
    if (attempts >= maxAttemptsFor(event, publisher)) {
      exhaust(event, publisher);
      return;
    }
    event.setNextAttemptOn(
        OffsetDateTime.now(ZoneOffset.UTC).plus(backoffFor(event, publisher, attempts)));
    log.warn(
        "Outbox relay attempt {} failed for row {} to {}: {}",
        attempts,
        event.getId(),
        event.getDestination(),
        event.getLastError());
  }

  private static int bookAttempt(OutboxEvent event, RuntimeException ex) {
    int attempts = event.getAttempts() + 1;
    event.setAttempts(attempts);
    event.setLastError(truncate(describe(ex)));
    return attempts;
  }

  private void exhaust(OutboxEvent event, OutboxPublisher publisher) {
    ExhaustionOutcome outcome =
        publisher == null ? ExhaustionOutcome.PARK : publisher.onExhausted(event);
    if (outcome == ExhaustionOutcome.DROP) {
      // given up: leaves the pending set as if relayed, forensics kept; NOT a delivery - no
      // listener fires, the relayed counter stays untouched
      event.setRelayedOn(OffsetDateTime.now(ZoneOffset.UTC));
      metrics.recordDropped(event.getDestination());
      log.warn(
          "Outbox row {} to {} DROPPED after {} attempts by its publisher's policy: {}",
          event.getId(),
          event.getDestination(),
          event.getAttempts(),
          event.getLastError());
      return;
    }
    event.setParkedOn(OffsetDateTime.now(ZoneOffset.UTC));
    log.error(
        "Outbox row {} to {} PARKED after {} attempts - ops action required (unpark after the"
            + " root cause is fixed): {}",
        event.getId(),
        event.getDestination(),
        event.getAttempts(),
        event.getLastError());
  }

  private int maxAttemptsFor(OutboxEvent event, OutboxPublisher publisher) {
    int declared = publisher == null ? 0 : publisher.maxAttempts(event);
    return declared > 0 ? declared : properties.getMaxAttempts();
  }

  private Duration backoffFor(OutboxEvent event, OutboxPublisher publisher, int attempts) {
    Duration declared = publisher == null ? null : publisher.backoff(event, attempts);
    return declared != null ? declared : backoff.delayFor(attempts);
  }

  private static String describe(RuntimeException ex) {
    return ex.getMessage() == null
        ? ex.toString()
        : ex.getClass().getSimpleName() + ": " + ex.getMessage();
  }

  private static String truncate(String text) {
    return text.length() <= LAST_ERROR_MAX_LENGTH
        ? text
        : text.substring(0, LAST_ERROR_MAX_LENGTH);
  }
}
