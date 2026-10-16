package org.opentmf.outbox.internal;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
 * One relay pass, CLAIMED BY LEASE (1.3.0): one short claim transaction per lane takes that
 * lane's due rows over its own query ({@code FOR UPDATE SKIP LOCKED}), resolves each row's
 * publisher, stamps {@code claimed_until} on the rows it takes and COMMITS. The sends then run
 * in NO transaction - CONCURRENT rows on the lane's threads, ORDERED rows one after another on
 * the ORDERED lane's one thread (1.8.0; the calling relay thread before, which held back the next
 * CONCURRENT claim for a whole ORDERED batch) - and each outcome is written by its own short
 * booking transaction,
 * guarded on the lease value this holder stamped: a holder whose lease lapsed (and whose row
 * another holder may own now) books nothing. At-least-once holds - a crash between delivery and
 * booking redelivers once the lease lapses, and the {@code x-idempotency-key} makes the
 * consumer's dedup trivial. No database connection is held while a send is in flight.
 *
 * <p>The lane is the one STAMPED on the row at append (null = ORDERED) - it wins even if the
 * publisher would now name another. The CLAIM takes: ORDERED rows up to {@code batch-size} (when
 * the relay says ORDERED rows may be due and the ORDERED lane is idle - one batch in flight per
 * pod); CONCURRENT rows up to the lane's ROOM - ahead of its
 * slots, into its hand-off queue of at most {@code batch-size} rows (1.6.0) - the query itself
 * keeping a key from going into flight twice. Neither lane's query reads the other lane's rows, so
 * no backlog of one can hide a row of the other. A claim's cost on the relay thread does not grow
 * with the rows it takes (1.6.0): ONE statement tries every candidate key's advisory lock, ONE
 * re-checks the locked candidates, and ONE stamps every lease of the same length.
 *
 * <p>The claim also moves {@code next_attempt_on} to the lease end: a pre-1.3.0 relay (which
 * knows no lease) then sees a leased row as not due during a rolling upgrade, and a lapsed lease
 * leaves the row due at the lapse for both. Every booking sets {@code next_attempt_on} again.
 *
 * <p>ORDERED rows are stamped with the SHORT ordered lease, and each row's lease is RENEWED in
 * the booking transaction of the row before it - so every lease covers one send, never the whole
 * batch; a row cancelled before its turn is released unsent. A CONCURRENT row that waited in the
 * hand-off queue for a tenth of its lease or longer is renewed the same way right before its send,
 * so every send starts with at least nine tenths of its lease.
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

  private static final Runnable NOTHING = () -> {};

  private final OutboxEventRepository repository;
  private final OutboxPublisherRouter router;
  private final OutboxBackoff backoff;
  private final OutboxMetrics metrics;
  private final OutboxProperties properties;
  private final List<OutboxRelayedListener> relayedListeners;
  /** The claims and the ORDERED bookings: READ COMMITTED whatever the application's default. */
  private final TransactionOperations tx;

  /**
   * The CONCURRENT lane's bookings and renewals: the application's default isolation (1.6.0) -
   * one guarded statement each, so the READ COMMITTED pin bought nothing there but a round trip.
   */
  private final TransactionOperations laneTx;

  private final OutboxConcurrentLane lane;

  /** The ORDERED lane's one thread (1.8.0): sends a claimed ORDERED batch off the relay thread. */
  private final OutboxOrderedLane orderedLane;

  /** The CONCURRENT claim's round-robin key cursor - read and moved on the relay thread only. */
  private String keyCursor = "";

  /** One claimed row: its publisher, the lease value this holder stamped, and when. */
  static final class Leased {
    final OutboxEvent event;
    final OutboxPublisher publisher;
    OffsetDateTime leasedAt;
    OffsetDateTime stamp;
    boolean lost;

    Leased(
        OutboxEvent event,
        OutboxPublisher publisher,
        OffsetDateTime leasedAt,
        OffsetDateTime stamp) {
      this.event = event;
      this.publisher = publisher;
      this.leasedAt = leasedAt;
      this.stamp = stamp;
    }

    /** The committed lease, mirrored on the (now detached) row. */
    void mirrorStamp() {
      event.setClaimedUntil(stamp);
      event.setNextAttemptOn(stamp); // a pre-1.3.0 relay sees the leased row as not due
    }
  }

  /** One lane's claim: the rows leased, and whether the query came back full (more may wait). */
  private record Claimed(List<Leased> rows, boolean full) {
    static final Claimed NONE = new Claimed(List.of(), false);
  }

  /**
   * One batch's outcome: the rows each lane claimed, and whether each lane's claim came back full
   * - more rows may be due, and the relay claims again at once.
   */
  record Batch(int ordered, int concurrent, boolean orderedFull, boolean concurrentFull) {

    /** The rows claimed over both lanes. */
    int claimed() {
      return ordered + concurrent;
    }
  }

  private record Attempt(Object result, RuntimeException failure) {}

  /**
   * Claims one batch and hands each lane its rows: the CONCURRENT rows to the lane's queue, the
   * ORDERED batch to the ORDERED lane's thread. Neither send runs on the calling thread, so the
   * caller's next claim never waits for one (1.8.0).
   *
   * @param withOrdered whether to claim the ORDERED lane - the relay's word that ORDERED rows may
   *     be due; ignored while the ORDERED lane still sends its previous batch
   */
  public Batch relayBatch(boolean withOrdered) {
    Claimed ordered = withOrdered && !orderedLane.busy() ? claimOrdered() : Claimed.NONE;
    Claimed concurrent = claimConcurrent();
    for (Leased row : concurrent.rows()) {
      if (!lane.submit(() -> relayConcurrent(row))) {
        log.info(
            "Outbox row {} not sent - the relay is stopping; its lease lapses",
            row.event.getId());
      }
    }
    List<Leased> orderedRows = ordered.rows();
    if (!orderedRows.isEmpty() && !orderedLane.submit(() -> relayOrdered(orderedRows))) {
      log.info(
          "Outbox ORDERED batch of {} rows not sent - the relay is stopping; their leases lapse",
          orderedRows.size());
    }
    return new Batch(
        orderedRows.size(), concurrent.rows().size(), ordered.full(), concurrent.full());
  }

  // ---------------------------------------------------------------- claim

  /** The ORDERED claim, its own short transaction on the relay thread; its failure fails it. */
  private Claimed claimOrdered() {
    Claimed claimed = tx.execute(status -> claimOrdered(now()));
    claimed.rows().forEach(Leased::mirrorStamp);
    return claimed;
  }

  /**
   * The CONCURRENT claim, its own short transaction on the relay thread - only when the lane has
   * room, and never more rows than that. A failed claim stamped nothing and lets the ORDERED rows
   * go on.
   */
  private Claimed claimConcurrent() {
    int room = lane.room();
    if (room == 0) {
      return Claimed.NONE;
    }
    try {
      Claimed claimed = tx.execute(status -> claimConcurrent(now(), room));
      claimed.rows().forEach(Leased::mirrorStamp);
      return claimed;
    } catch (RuntimeException ex) {
      log.error("Outbox CONCURRENT claim failed; the sweep will retry", ex);
      return Claimed.NONE;
    }
  }

  private Claimed claimOrdered(OffsetDateTime now) {
    int limit = properties.getBatchSize();
    List<OutboxEvent> rows = repository.claimOrdered(now, Lane.CONCURRENT, Limit.of(limit));
    List<Leased> leased = new ArrayList<>();
    for (OutboxEvent row : rows) {
      OutboxPublisher publisher = route(row);
      if (publisher != null) {
        leased.add(lease(row, publisher, Lane.ORDERED, now));
      }
    }
    stampAll(leased);
    return new Claimed(leased, rows.size() >= limit);
  }

  /**
   * Claims up to {@code limit} CONCURRENT rows: keys round-robin from the cursor (then wrapping
   * round to the cursor), rows without a key in {@code id} order. The cursor moves to the last
   * key claimed, so every key gets its turn whatever the others' backlogs.
   */
  private Claimed claimConcurrent(OffsetDateTime now, int limit) {
    List<OutboxEvent> rows = new ArrayList<>(repository.claimConcurrent(now, keyCursor, limit));
    String lastKey = lastKey(rows);
    if (rows.size() < limit && !keyCursor.isEmpty()) {
      List<OutboxEvent> wrapped =
          repository.claimConcurrentWrap(now, keyCursor, limit - rows.size());
      String wrappedLast = lastKey(wrapped);
      lastKey = wrappedLast != null ? wrappedLast : lastKey;
      rows.addAll(wrapped);
    }
    List<Leased> leased = new ArrayList<>();
    for (OutboxEvent row : acrossPods(rows, now)) {
      OutboxPublisher publisher = route(row);
      if (publisher != null) {
        leased.add(lease(row, publisher, Lane.CONCURRENT, now));
      }
    }
    stampAll(leased);
    if (lastKey != null) {
      keyCursor = lastKey;
    }
    return new Claimed(leased, rows.size() >= limit);
  }

  /**
   * The cross-pod guard of the keyed candidates (PostgreSQL advisory locks, by ruling): ONE
   * statement TRIES the transaction-scoped lock of every candidate key - a key another pod is
   * claiming right now is skipped this pass, never waited for - and ONE re-checks the locked
   * candidates in a NEW snapshot, which sees every lease committed before a lock was released.
   * Unkeyed candidates pass as they are. Rows of one key are thus never in flight together, within
   * a pod and across pods.
   */
  private List<OutboxEvent> acrossPods(List<OutboxEvent> candidates, OffsetDateTime now) {
    List<String> keys =
        candidates.stream()
            .map(OutboxEvent::getOrderingKey)
            .filter(Objects::nonNull)
            .distinct()
            .toList();
    if (keys.isEmpty()) {
      return candidates;
    }
    Set<String> locked =
        new HashSet<>(repository.tryLockKeys(OutboxClaimSql.KEY_LOCK_NAMESPACE, keysLiteral(keys)));
    List<OutboxEvent> keyed =
        candidates.stream()
            .filter(row -> row.getOrderingKey() != null && locked.contains(row.getOrderingKey()))
            .toList();
    Set<Long> stillHeads =
        keyed.isEmpty()
            ? Set.of()
            : new HashSet<>(repository.recheckKeyedHeads(now, idsLiteral(keyed)));
    return candidates.stream()
        .filter(row -> row.getOrderingKey() == null || stillHeads.contains(row.getId()))
        .toList();
  }

  /**
   * Writes the leases of one claim: ONE statement per distinct lease end - a claim's rows share
   * one unless their publishers declare different leases - never one per row. The rows are locked
   * by the claim; the entities are left untouched here (no per-row flush) and mirror the stamp
   * once the claim has committed.
   */
  private void stampAll(List<Leased> leased) {
    Map<OffsetDateTime, List<OutboxEvent>> byStamp = new LinkedHashMap<>();
    for (Leased row : leased) {
      byStamp.computeIfAbsent(row.stamp, stamp -> new ArrayList<>()).add(row.event);
    }
    byStamp.forEach((stamp, rows) -> repository.stampLeases(stamp, idsLiteral(rows)));
  }

  private static String idsLiteral(List<OutboxEvent> rows) {
    return rows.stream()
        .map(row -> String.valueOf(row.getId()))
        .collect(Collectors.joining(",", "{", "}"));
  }

  /**
   * The keys as a PostgreSQL array literal: every element double-quoted, its backslashes and
   * double quotes backslash-escaped - so any key (commas, braces, quotes, spaces) is one element.
   */
  static String keysLiteral(List<String> keys) {
    return keys.stream()
        .map(key -> '"' + key.replace("\\", "\\\\").replace("\"", "\\\"") + '"')
        .collect(Collectors.joining(",", "{", "}"));
  }

  /**
   * The highest key claimed - in Java {@link String} order, which is the database's key order
   * because {@code ordering_key} is {@code collate "C"} (byte order; the two differ only for
   * supplementary characters, a fairness skew at worst - every key is still reached).
   */
  private static String lastKey(List<OutboxEvent> rows) {
    return rows.stream()
        .map(OutboxEvent::getOrderingKey)
        .filter(Objects::nonNull)
        .max(Comparator.naturalOrder())
        .orElse(null);
  }

  /** The row's publisher - or null after booking the routing failure by the LIBRARY policy. */
  private OutboxPublisher route(OutboxEvent row) {
    try {
      return router.resolve(row);
    } catch (RuntimeException ex) {
      registerFailure(row, null, ex);
      return null;
    }
  }

  private Leased lease(
      OutboxEvent row, OutboxPublisher publisher, Lane rowLane, OffsetDateTime now) {
    return new Leased(row, publisher, now, leaseEnd(row, publisher, rowLane, now));
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

  /**
   * One CONCURRENT row, on a lane worker: renewed first if it waited long in the hand-off queue,
   * then sent and booked.
   */
  private void relayConcurrent(Leased row) {
    try {
      if (waitedLong(row) && !renewQueued(row)) {
        return; // taken by another holder meanwhile, or cancelled and released unsent
      }
      book(laneTx, row, send(row), () -> NOTHING);
    } catch (RuntimeException ex) {
      log.error(
          "Outbox row {} to {} - renewal or booking failed; the row is redelivered once its"
              + " lease lapses",
          row.event.getId(),
          row.event.getDestination(),
          ex);
    }
  }

  /** Whether a queued row has waited a tenth of its lease or longer since it was leased. */
  private static boolean waitedLong(Leased row) {
    Duration lease = Duration.between(row.leasedAt, row.stamp);
    return Duration.between(row.leasedAt, now()).compareTo(lease.dividedBy(10)) >= 0;
  }

  /**
   * Renews a queued row's lease in its own short transaction, as the ORDERED lane renews the next
   * row: guarded on the stamp; a row lost meanwhile is not sent, a cancelled one is released
   * unsent.
   *
   * @return whether the row is still this holder's to send
   */
  private boolean renewQueued(Leased row) {
    commit(
        laneTx,
        () -> {
          List<Runnable> afterCommit = new ArrayList<>();
          renew(row, Lane.CONCURRENT, afterCommit);
          return () -> afterCommit.forEach(Runnable::run);
        },
        () -> NOTHING);
    return !row.lost;
  }

  private void relayOrdered(List<Leased> ordered) {
    for (int i = 0; i < ordered.size(); i++) {
      Leased row = ordered.get(i);
      if (row.lost) {
        continue;
      }
      int index = i;
      try {
        book(tx, row, send(row), () -> renewNext(ordered, index));
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
      renewed = !next.lost && renew(next, Lane.ORDERED, afterCommit);
    }
    return () -> afterCommit.forEach(Runnable::run);
  }

  /** Renews one row's lease; {@code false} when the row is lost or cancelled (released). */
  private boolean renew(Leased next, Lane rowLane, List<Runnable> afterCommit) {
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
    OffsetDateTime now = now();
    OffsetDateTime renewed = leaseEnd(row, next.publisher, rowLane, now);
    row.setClaimedUntil(renewed);
    row.setNextAttemptOn(renewed);
    afterCommit.add(
        () -> {
          next.leasedAt = now;
          next.stamp = renewed;
        });
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
  private void book(
      TransactionOperations via, Leased row, Attempt attempt, Supplier<Runnable> alsoInTx) {
    RuntimeException failure = attempt.failure();
    if (failure == null) {
      try {
        commit(via, () -> bookRelayed(row, attempt.result()), alsoInTx);
        return;
      } catch (RuntimeException refused) {
        failure = refused;
      }
    }
    RuntimeException failed = failure;
    commit(via, () -> bookFailure(row, failed), alsoInTx);
  }

  private static void commit(
      TransactionOperations via, Supplier<Runnable> booking, Supplier<Runnable> alsoInTx) {
    via.execute(status -> List.of(booking.get(), alsoInTx.get())).forEach(Runnable::run);
  }

  /**
   * The RELAYED booking: ONE guarded statement (1.6.0) clears the lease and stamps
   * {@code relayed_on} - {@code next_attempt_on} with it, no longer the lease end - and returns the
   * row it booked, managed, for the hook and the listeners; an empty result means the lease lapsed.
   */
  private Runnable bookRelayed(Leased leased, Object result) {
    Optional<OutboxEvent> held =
        repository.bookRelayed(leased.event.getId(), leased.stamp, now());
    if (held.isEmpty()) {
      logLapsed(leased);
      return NOTHING;
    }
    OutboxEvent row = held.get();
    if (row.getCancelledOn() != null) {
      log.warn(
          "Outbox row {} to {} was cancelled after its lease lapsed mid-send and the send"
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
      row.setNextAttemptOn(now()); // no longer the lease end; never claimed again anyway
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
