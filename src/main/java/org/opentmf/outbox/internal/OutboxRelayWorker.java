package org.opentmf.outbox.internal;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opentmf.outbox.OutboxBooking;
import org.opentmf.outbox.OutboxBooking.Outcome;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxPublisher.ExhaustionOutcome;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.opentmf.outbox.OutboxRelayedListener;
import org.opentmf.outbox.TerminalOutboxException;
import org.springframework.data.domain.Limit;
import org.springframework.transaction.support.TransactionOperations;

/**
 * One relay pass, CLAIMED BY LEASE (1.3.0): a short claim transaction scans the due rows in
 * {@code id} order, resolves each row's publisher and lane, stamps {@code claimed_until} on the
 * rows it takes and COMMITS. The sends then run in NO transaction - CONCURRENT rows on the
 * lane's threads, ORDERED rows one after another on the calling relay thread - and each outcome
 * is written by its own short booking transaction, guarded on the lease value this holder
 * stamped: a holder whose lease lapsed (and whose row another holder may own now) books
 * nothing. At-least-once holds - a crash between delivery and booking redelivers once the lease
 * lapses, and the {@code x-idempotency-key} makes the consumer's dedup trivial. No database
 * connection is held while a send is in flight.
 *
 * <p>The CLAIM takes: ORDERED rows up to {@code batch-size}; CONCURRENT rows only while a lane
 * slot is free (the slot is taken before the row is stamped, so no lease burns in a queue), and
 * per ordering key only the first row - never one whose key is already in flight. It scans up to
 * {@link #MAX_CLAIM_WINDOWS} windows of {@code batch-size} rows, so an ORDERED row is found even
 * behind a backlog of CONCURRENT rows waiting for slots.
 *
 * <p>ORDERED rows are stamped with the SHORT ordered lease, and each row's lease is RENEWED in
 * the booking transaction of the row before it - so every lease covers one send, never the whole
 * batch; a row cancelled before its turn is released unsent.
 *
 * <p>On failure: {@code attempts++}, {@code last_error} recorded, {@code next_attempt_on}
 * pushed out by the publisher's backoff (library exponential backoff by default); at the
 * publisher's budget (library {@code max-attempts} by default) the row is EXHAUSTED: PARK
 * stamps {@code parked_on} (excluded from claims, the {@code parked} gauge alerts, unparking
 * is an explicit ops action) or DROP stamps {@code relayed_on} with the forensics kept (no
 * listener fires, the {@code dropped} counter books it). A {@link TerminalOutboxException}
 * reaches exhaustion immediately. The failure bookkeeping NEVER touches {@code release_at}: the
 * scheduled-send hold and the retry schedule are separate facts (the entity maps the hold
 * {@code updatable = false}, and a regression test names the property).
 */
@Slf4j
@RequiredArgsConstructor
class OutboxRelayWorker {

  /** Bound for the persisted {@code last_error} text - keeps forensic rows sane. */
  static final int LAST_ERROR_MAX_LENGTH = 4000;

  /** Windows of {@code batch-size} rows one claim scans at most. */
  static final int MAX_CLAIM_WINDOWS = 10;

  private static final Runnable NOTHING = () -> {};

  private final OutboxEventRepository repository;
  private final OutboxPublisherRouter router;
  private final OutboxBackoff backoff;
  private final OutboxMetrics metrics;
  private final OutboxProperties properties;
  private final List<OutboxRelayedListener> relayedListeners;
  private final TransactionOperations tx;
  private final OutboxConcurrentLane lane;

  /** One claimed row: its publisher and the lease value this holder stamped. */
  static final class Leased {
    final OutboxEvent event;
    final OutboxPublisher publisher;
    OffsetDateTime stamp;
    boolean lost;

    Leased(OutboxEvent event, OutboxPublisher publisher, OffsetDateTime stamp) {
      this.event = event;
      this.publisher = publisher;
      this.stamp = stamp;
    }
  }

  private record Claim(List<Leased> ordered, List<Leased> concurrent) {}

  private record Attempt(Object result, RuntimeException failure) {}

  /**
   * Claims one batch, starts its CONCURRENT sends and relays its ORDERED rows.
   *
   * @return the number of rows claimed - the relay drains while this reaches the batch size
   */
  public int relayBatch() {
    Claim claim = claim();
    for (Leased row : claim.concurrent()) {
      if (!lane.submit(() -> relayConcurrent(row))) {
        log.info(
            "Outbox row {} not sent - the relay is stopping; its lease lapses",
            row.event.getId());
      }
    }
    relayOrdered(claim.ordered());
    return claim.ordered().size() + claim.concurrent().size();
  }

  // ---------------------------------------------------------------- claim

  private Claim claim() {
    List<Leased> concurrent = new ArrayList<>();
    try {
      return tx.execute(status -> scan(concurrent));
    } catch (RuntimeException ex) {
      lane.releaseUnused(concurrent.size()); // the stamps never committed
      throw ex;
    }
  }

  private Claim scan(List<Leased> concurrent) {
    OffsetDateTime now = now();
    int batch = properties.getBatchSize();
    List<Leased> ordered = new ArrayList<>();
    Set<String> takenKeys = new HashSet<>();
    long after = Long.MIN_VALUE;
    for (int window = 0; window < MAX_CLAIM_WINDOWS; window++) {
      List<OutboxEvent> rows = repository.claimWindow(now, after, Limit.of(batch));
      for (OutboxEvent row : rows) {
        after = row.getId();
        consider(row, now, ordered, concurrent, takenKeys);
      }
      if (rows.size() < batch || (ordered.size() >= batch && lane.available() == 0)) {
        break;
      }
    }
    return new Claim(ordered, concurrent);
  }

  private void consider(
      OutboxEvent row,
      OffsetDateTime now,
      List<Leased> ordered,
      List<Leased> concurrent,
      Set<String> takenKeys) {
    boolean inFlight = row.getClaimedUntil() != null && row.getClaimedUntil().isAfter(now);
    OutboxPublisher publisher = null;
    Lane rowLane;
    String key;
    try {
      publisher = router.resolve(row);
      rowLane = publisher.lane(row);
      key = rowLane == Lane.CONCURRENT ? publisher.orderingKey(row) : null;
    } catch (RuntimeException ex) {
      if (!inFlight) {
        registerFailure(row, publisher, ex); // unroutable: the LIBRARY policy books it
      }
      return;
    }
    if (rowLane == Lane.CONCURRENT) {
      // a key seen once in this scan is taken - by a row in flight, by the row claimed here, or
      // by an earlier row left waiting for a slot: later rows of the key wait their turn
      boolean keyFree = key == null || takenKeys.add(key);
      if (!inFlight && keyFree && lane.tryAcquire()) {
        concurrent.add(stamp(row, publisher, Lane.CONCURRENT, now));
      }
    } else if (!inFlight && ordered.size() < properties.getBatchSize()) {
      ordered.add(stamp(row, publisher, Lane.ORDERED, now));
    }
  }

  private Leased stamp(
      OutboxEvent row, OutboxPublisher publisher, Lane rowLane, OffsetDateTime now) {
    OffsetDateTime stamp = leaseEnd(row, publisher, rowLane, now);
    row.setClaimedUntil(stamp);
    return new Leased(row, publisher, stamp);
  }

  /** {@code now + lease}, truncated to the column's microsecond precision so the guard matches. */
  private OffsetDateTime leaseEnd(
      OutboxEvent row, OutboxPublisher publisher, Lane rowLane, OffsetDateTime now) {
    Duration lease = publisher.lease(row);
    if (lease == null) {
      lease =
          rowLane == Lane.CONCURRENT ? properties.getLease() : properties.getOrdered().getLease();
    }
    return now.plus(lease).truncatedTo(ChronoUnit.MICROS);
  }

  // ---------------------------------------------------------------- lanes

  private void relayConcurrent(Leased row) {
    try {
      book(row, send(row), () -> NOTHING);
    } catch (RuntimeException ex) {
      log.error(
          "Outbox row {} to {} - booking failed; the row is redelivered once its lease lapses",
          row.event.getId(),
          row.event.getDestination(),
          ex);
    } finally {
      lane.release();
    }
  }

  private void relayOrdered(List<Leased> ordered) {
    for (int i = 0; i < ordered.size(); i++) {
      Leased row = ordered.get(i);
      if (row.lost) {
        continue;
      }
      int index = i;
      try {
        book(row, send(row), () -> renewNext(ordered, index));
      } catch (RuntimeException ex) {
        log.error(
            "Outbox row {} to {} - booking failed; it and the rest of the batch are redelivered"
                + " once their leases lapse",
            row.event.getId(),
            row.event.getDestination(),
            ex);
        return;
      }
    }
  }

  /**
   * Inside the booking transaction of ORDERED row {@code index}: renews the lease of the next
   * row still held, right before its send. A row whose lease lapsed and was taken is lost; a row
   * cancelled meanwhile is released unsent. The in-memory stamp moves only after the commit.
   */
  private Runnable renewNext(List<Leased> ordered, int index) {
    List<Runnable> afterCommit = new ArrayList<>();
    boolean renewed = false;
    for (int j = index + 1; j < ordered.size() && !renewed; j++) {
      Leased next = ordered.get(j);
      renewed = !next.lost && renew(next, afterCommit);
    }
    return () -> afterCommit.forEach(Runnable::run);
  }

  /** Renews one row's lease; {@code false} when the row is lost or cancelled (released). */
  private boolean renew(Leased next, List<Runnable> afterCommit) {
    Optional<OutboxEvent> held = repository.lockLeased(next.event.getId(), next.stamp);
    if (held.isEmpty()) {
      afterCommit.add(() -> next.lost = true);
      return false;
    }
    OutboxEvent row = held.get();
    if (row.getCancelledOn() != null) {
      row.setClaimedUntil(null);
      afterCommit.add(() -> next.lost = true);
      log.info("Outbox row {} cancelled before its send - released unsent", row.getId());
      return false;
    }
    OffsetDateTime renewed = leaseEnd(row, next.publisher, Lane.ORDERED, now());
    row.setClaimedUntil(renewed);
    afterCommit.add(() -> next.stamp = renewed);
    return true;
  }

  private static Attempt send(Leased row) {
    try {
      return new Attempt(row.publisher.deliver(row.event), null);
    } catch (RuntimeException ex) {
      return new Attempt(null, ex);
    }
  }

  // ---------------------------------------------------------------- booking

  /**
   * Books one attempt in its own short transaction; {@code alsoInTx} rides the same transaction
   * and returns what to apply once it committed. A refused RELAYED booking (a hook or listener
   * threw - its writes roll back with the stamp) is booked as an ordinary failure instead.
   */
  private void book(Leased row, Attempt attempt, Supplier<Runnable> alsoInTx) {
    RuntimeException failure = attempt.failure();
    if (failure == null) {
      try {
        commit(() -> bookRelayed(row, attempt.result()), alsoInTx);
        return;
      } catch (RuntimeException refused) {
        failure = refused;
      }
    }
    RuntimeException failed = failure;
    commit(() -> bookFailure(row, failed), alsoInTx);
  }

  private void commit(Supplier<Runnable> booking, Supplier<Runnable> alsoInTx) {
    tx.execute(status -> List.of(booking.get(), alsoInTx.get())).forEach(Runnable::run);
  }

  private Runnable bookRelayed(Leased leased, Object result) {
    Optional<OutboxEvent> held = repository.lockLeased(leased.event.getId(), leased.stamp);
    if (held.isEmpty()) {
      logLapsed(leased);
      return NOTHING;
    }
    OutboxEvent row = held.get();
    row.setClaimedUntil(null);
    row.setRelayedOn(now());
    if (row.getCancelledOn() != null) {
      log.warn(
          "Outbox row {} to {} was cancelled while its send was in flight and the send"
              + " succeeded - booked SENT-BUT-CANCELLED (both stamps)",
          row.getId(),
          row.getDestination());
    }
    // the booking hook, then the post-relay seam: both commit with relayed_on, or neither
    leased.publisher.onBooked(row, OutboxBooking.relayed(result));
    for (OutboxRelayedListener listener : relayedListeners) {
      listener.onRelayed(row);
    }
    String destination = row.getDestination();
    int tries = row.getAttempts() + 1;
    return () -> metrics.recordRelayed(destination, tries);
  }

  private Runnable bookFailure(Leased leased, RuntimeException ex) {
    Optional<OutboxEvent> held = repository.lockLeased(leased.event.getId(), leased.stamp);
    if (held.isEmpty()) {
      logLapsed(leased);
      return NOTHING;
    }
    OutboxEvent row = held.get();
    row.setClaimedUntil(null);
    OutboxBooking booking;
    if (row.getCancelledOn() != null) {
      bookAttempt(row, ex);
      booking = OutboxBooking.failed(Outcome.CANCELLED, ex, null);
      log.info(
          "Outbox row {} - send failed after it was cancelled; it retires cancelled: {}",
          row.getId(),
          row.getLastError());
    } else {
      booking = registerFailure(row, leased.publisher, ex);
    }
    leased.publisher.onBooked(row, booking);
    String destination = row.getDestination();
    return booking.exhaustion() == ExhaustionOutcome.DROP
        ? () -> metrics.recordDropped(destination)
        : NOTHING;
  }

  private static void logLapsed(Leased leased) {
    log.warn(
        "Outbox row {} to {} - the lease stamped {} lapsed before the booking; another holder"
            + " owns the row, nothing booked here (the receiver dedups on the idempotency key)."
            + " Is the publisher's lease shorter than its longest call?",
        leased.event.getId(),
        leased.event.getDestination(),
        leased.stamp);
  }

  private OutboxBooking registerFailure(
      OutboxEvent event, OutboxPublisher publisher, RuntimeException ex) {
    int attempts = bookAttempt(event, ex);
    if (ex instanceof TerminalOutboxException || attempts >= maxAttemptsFor(event, publisher)) {
      return OutboxBooking.failed(Outcome.EXHAUSTED, ex, exhaust(event, publisher));
    }
    event.setNextAttemptOn(now().plus(backoffFor(event, publisher, attempts)));
    log.warn(
        "Outbox relay attempt {} failed for row {} to {}: {}",
        attempts,
        event.getId(),
        event.getDestination(),
        event.getLastError());
    return OutboxBooking.failed(Outcome.RETRY, ex, null);
  }

  private static int bookAttempt(OutboxEvent event, RuntimeException ex) {
    int attempts = event.getAttempts() + 1;
    event.setAttempts(attempts);
    event.setLastError(truncate(describe(ex)));
    return attempts;
  }

  private ExhaustionOutcome exhaust(OutboxEvent event, OutboxPublisher publisher) {
    ExhaustionOutcome outcome =
        publisher == null ? ExhaustionOutcome.PARK : publisher.onExhausted(event);
    if (outcome == ExhaustionOutcome.DROP) {
      // given up: leaves the pending set as if relayed, forensics kept; NOT a delivery - no
      // listener fires, the relayed counter stays untouched
      event.setRelayedOn(now());
      log.warn(
          "Outbox row {} to {} DROPPED after {} attempts by its publisher's policy: {}",
          event.getId(),
          event.getDestination(),
          event.getAttempts(),
          event.getLastError());
      return ExhaustionOutcome.DROP;
    }
    event.setParkedOn(now());
    log.error(
        "Outbox row {} to {} PARKED after {} attempts - ops action required (unpark after the"
            + " root cause is fixed): {}",
        event.getId(),
        event.getDestination(),
        event.getAttempts(),
        event.getLastError());
    return ExhaustionOutcome.PARK;
  }

  private int maxAttemptsFor(OutboxEvent event, OutboxPublisher publisher) {
    int declared = publisher == null ? 0 : publisher.maxAttempts(event);
    return declared > 0 ? declared : properties.getMaxAttempts();
  }

  private Duration backoffFor(OutboxEvent event, OutboxPublisher publisher, int attempts) {
    Duration declared = publisher == null ? null : publisher.backoff(event, attempts);
    return declared != null ? declared : backoff.delayFor(attempts);
  }

  private static OffsetDateTime now() {
    return OffsetDateTime.now(ZoneOffset.UTC);
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
