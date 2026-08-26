package org.opentmf.outbox;

import java.time.OffsetDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Limit;
import org.springframework.transaction.annotation.Transactional;

/**
 * One S23 relay pass: claims a batch of due pending rows ({@code FOR UPDATE SKIP LOCKED}, id
 * order), routes each through the publisher SPI and books the outcome - all in ONE claim
 * transaction. On success {@code relayed_on} is set; a crash between delivery and commit means
 * redelivery - that is the at-least-once contract, and the {@code x-idempotency-key} makes
 * consumer dedup trivial.
 *
 * <p>On failure: {@code attempts++}, {@code last_error} recorded, {@code next_attempt_on}
 * pushed out by exponential backoff; at {@code max-attempts} the row PARKS (excluded from
 * claims, the {@code parked} gauge alerts, unparking is an explicit ops action). A failed row
 * does not stop the batch.
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
            OffsetDateTime.now(), properties.getMaxAttempts(), Limit.of(properties.getBatchSize()));
    claimed.forEach(this::relayOne);
    return claimed.size();
  }

  private void relayOne(OutboxEvent event) {
    try {
      router.publish(event);
      event.setRelayedOn(OffsetDateTime.now());
      metrics.recordRelayed(event.getDestination(), event.getAttempts() + 1);
    } catch (RuntimeException ex) {
      registerFailure(event, ex);
    }
  }

  private void registerFailure(OutboxEvent event, RuntimeException ex) {
    int attempts = event.getAttempts() + 1;
    event.setAttempts(attempts);
    event.setLastError(truncate(describe(ex)));
    event.setNextAttemptOn(OffsetDateTime.now().plus(backoff.delayFor(attempts)));
    if (attempts >= properties.getMaxAttempts()) {
      log.error(
          "Outbox row {} to {} PARKED after {} attempts - ops action required (unpark after the"
              + " root cause is fixed): {}",
          event.getId(),
          event.getDestination(),
          attempts,
          event.getLastError());
    } else {
      log.warn(
          "Outbox relay attempt {} failed for row {} to {}: {}",
          attempts,
          event.getId(),
          event.getDestination(),
          event.getLastError());
    }
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
