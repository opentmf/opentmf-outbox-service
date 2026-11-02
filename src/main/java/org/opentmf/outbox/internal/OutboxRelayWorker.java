package org.opentmf.outbox.internal;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opentmf.outbox.OutboxBooking;
import org.opentmf.outbox.OutboxBooking.Outcome;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxLeaseLapsedException;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxPublisher.ExhaustionOutcome;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.opentmf.outbox.OutboxRelayedListener;
import org.opentmf.outbox.OutboxSendError;
import org.opentmf.outbox.TerminalOutboxException;
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
 * pod); CONCURRENT rows up to the lane's ROOM - ahead of its slots, into its hand-off queue as
 * deep as the sends allow (1.6.0, 1.9.0), never a row this pod still holds queued - the query
 * itself keeping a key from going into flight twice. Neither lane's query reads the other lane's rows, so
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
 * hand-off queue for a tenth of its lease or longer is leased again the same way when it leaves
 * the queue, right before its send, so every send starts with at least nine tenths of its lease -
 * and a pod never claims a row it still holds queued, so a lease that lapsed in the queue costs
 * the row nothing but the renewal (1.9.0).
 *
 * <p>WHILE a send runs, the {@link OutboxSendHeartbeat} keeps its lease - up to the send's bound,
 * the larger of its lease and {@code send-timeout} (1.9.0) - and marks the send as under way
 * ({@code sending_since}). A lease that lapses on a row whose send was under way (its pod died, or
 * the send outran its bound) COUNTS as a failed attempt when the next claim takes the row: booked
 * like any failure ({@link OutboxLeaseLapsedException} - backoff, then the publisher's budget
 * parks or drops it), the row not sent on that claim. A send that never ends thus exhausts its
 * row instead of being re-sent every lease forever, and its ORDERED lane or its key moves on.
 * A lease that lapses on a row held UNSENT (queued, behind an ORDERED head) costs it nothing.
 *
 * <p>Anything a publisher, a booking hook or a relayed listener throws - an {@link Error}
 * included - is a failed attempt under the same policy (1.9.0; an {@code Error} as the cause of
 * an {@link OutboxSendError}); a failure booking whose hook throws again is booked without the
 * hook, so the attempt counts. No throwable leaves a relay thread.
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

  /** The database's time (1.9.0): every lease, due time and booking instant. */
  private final OutboxClock clock;

  /** Keeps the lease of every send in flight, up to its bound (1.9.0). */
  private final OutboxSendHeartbeat heartbeat;

  /** The CONCURRENT claim's round-robin key cursor - read and moved on the relay thread only. */
  private String keyCursor = "";

  /**
   * The CONCURRENT rows this pod holds in the lane's queue, not yet sending (1.9.0): a claim never
   * takes one of them again. Up to 1.8.0 a queued row whose lease lapsed was claimed again by the
   * next pass and queued at the TAIL - and its old place was dropped as lost.
   */
  private final Map<Long, Leased> queuedHere = new ConcurrentHashMap<>();

  /**
   * One claimed row: its publisher, the lease value this holder stamped, and when - and, while it
   * is sent, what the {@link OutboxSendHeartbeat} needs (under {@link #lock}, which the booking
   * takes too).
   */
  static final class Leased {
    final OutboxEvent event;
    final OutboxPublisher publisher;
    final Duration leaseLength;

    /** The previous holder's lease lapsed while its send was under way: this claim counts it. */
    final boolean lapsedSending;

    final ReentrantLock lock = new ReentrantLock();
    OffsetDateTime leasedAt;
    volatile OffsetDateTime stamp;
    volatile boolean lost;

    /** Whether the send is marked under way in the database ({@code sending_since}). */
    volatile boolean marked;

    boolean sending;
    boolean interrupted;
    Instant sendStartedAt;
    Thread sender;

    Leased(
        OutboxEvent event,
        OutboxPublisher publisher,
        OffsetDateTime leasedAt,
        OffsetDateTime stamp,
        boolean lapsedSending) {
      this.event = event;
      this.publisher = publisher;
      this.leasedAt = leasedAt;
      this.stamp = stamp;
      this.leaseLength = Duration.between(leasedAt, stamp);
      this.lapsedSending = lapsedSending;
    }

    /** The committed lease, mirrored on the (now detached) row. */
    void mirrorStamp() {
      event.setClaimedUntil(stamp);
      event.setNextAttemptOn(stamp); // a pre-1.3.0 relay sees the leased row as not due
      event.setSendingSince(null);
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

  /** A throwable as a booking's failure: an {@link Error} (or a sneaky checked one) wrapped. */
  static RuntimeException asFailure(Throwable thrown) {
    return thrown instanceof RuntimeException runtime ? runtime : new OutboxSendError(thrown);
  }

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
      queuedHere.put(row.event.getId(), row);
      if (!lane.submit(row.event.getId(), () -> relayConcurrent(row))) {
        queuedHere.remove(row.event.getId());
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
    Claimed claimed = tx.execute(status -> claimOrdered(clock.now()));
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
      Claimed claimed = tx.execute(status -> claimConcurrent(clock.now(), room));
      claimed.rows().forEach(Leased::mirrorStamp);
      return claimed;
    } catch (RuntimeException ex) {
      log.error("Outbox CONCURRENT claim failed; the sweep will retry", ex);
      return Claimed.NONE;
    }
  }

  private Claimed claimOrdered(OffsetDateTime now) {
    int limit = properties.getBatchSize();
    List<OutboxEvent> rows = repository.claimOrdered(now, limit);
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
   *
   * <p>A first pass that comes back short wraps round only when the keys above the cursor RAN
   * OUT. It also comes back short when another pod's claim holds some of its heads locked ({@code
   * SKIP LOCKED}); a wrap then would move the cursor back to the first keys, and every key above -
   * due all along - would lose its turn for a whole cycle (OUTBOX-D3-FAIRNESS-INVESTIGATION-1: on
   * a slow host a contiguous block of rows waited minutes). So a short pass reads the heads it
   * would have found; with as many as it asked for, it does not wrap and the cursor moves on to
   * the last of them - past the locked ones too: the other pod is claiming them, and a head held
   * locked for longer (a consumer's transaction) never stops the cursor.
   */
  private Claimed claimConcurrent(OffsetDateTime now, int limit) {
    List<OutboxEvent> rows = new ArrayList<>(repository.claimConcurrent(now, keyCursor, limit));
    String lastKey = lastKey(rows);
    List<String> walked =
        rows.size() < limit && !keyCursor.isEmpty()
            ? repository.concurrentHeadKeys(now, keyCursor, limit)
            : List.of();
    if (walked.size() >= limit) {
      lastKey = walked.stream().max(Comparator.naturalOrder()).orElseThrow();
    } else if (rows.size() < limit && !keyCursor.isEmpty()) {
      List<OutboxEvent> wrapped =
          repository.claimConcurrentWrap(now, keyCursor, limit - rows.size());
      String wrappedLast = lastKey(wrapped);
      lastKey = wrappedLast != null ? wrappedLast : lastKey;
      rows.addAll(wrapped);
    }
    List<Leased> leased = new ArrayList<>();
    for (OutboxEvent row : acrossPods(rows, now)) {
      if (queuedHere.containsKey(row.getId())) {
        continue; // still queued here, its lease lapsed: it keeps its place, never a second one
      }
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
  @SuppressWarnings("java:S1181") // an Error is a failed attempt; nothing leaves the thread (F-4)
  private OutboxPublisher route(OutboxEvent row) {
    try {
      return router.resolve(row);
    } catch (Throwable ex) { // a router or a publisher's supports() may throw anything
      registerFailure(row, null, asFailure(ex));
      return null;
    }
  }

  /**
   * Leases one claimed row. Read BEFORE the stamp is written: a row whose previous lease lapsed
   * while its send was under way - its pod died mid-send, or the send outran its bound - carries
   * that lease still, and {@code sending_since}; this claim then books the lapse as a failed
   * attempt instead of sending (see {@link #relayConcurrent}).
   */
  private Leased lease(
      OutboxEvent row, OutboxPublisher publisher, Lane rowLane, OffsetDateTime now) {
    boolean lapsedSending = row.getClaimedUntil() != null && row.getSendingSince() != null;
    return new Leased(row, publisher, now, leaseEnd(row, publisher, rowLane, now), lapsedSending);
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
  @SuppressWarnings("java:S1181") // an Error is a failed attempt; nothing leaves the thread (F-4)
  private void relayConcurrent(Leased row) {
    queuedHere.remove(row.event.getId()); // it leaves the queue: from here on a claim may see it
    try {
      if (row.lapsedSending) {
        book(laneTx, row, lapsed(row), () -> NOTHING); // counted, not sent: backoff first
        return;
      }
      if (waitedLong(row) && !renewQueued(row)) {
        return; // taken by another holder meanwhile, or cancelled and released unsent
      }
      book(laneTx, row, send(row), () -> NOTHING);
    } catch (Throwable ex) { // nothing leaves a lane worker
      log.error(
          "Outbox row {} to {} - renewal or booking failed; the row is redelivered once its"
              + " lease lapses",
          row.event.getId(),
          row.event.getDestination(),
          ex);
    }
  }

  /**
   * Gives back the rows this pod holds queued while NO slot of its lane is live (1.9.0): each slot
   * busy with a send older than a second, they would only wait behind them - claimed in the second
   * before the slots turned slow. Their leases are released in ONE guarded statement, due at once,
   * for any pod with a free slot (this one included, once a slot frees). Runs on the heartbeat's
   * tick; a failed release leaves the leases to lapse - the rows were never sent, so it costs them
   * nothing.
   */
  void giveBackStranded() {
    List<Leased> rows =
        lane.evictStranded().stream().map(queuedHere::remove).filter(Objects::nonNull).toList();
    if (rows.isEmpty()) {
      return;
    }
    try {
      Integer released =
          laneTx.execute(
              status ->
                  repository.releaseLeases(
                      OutboxSqlArrays.ids(rows.stream().map(r -> r.event.getId()).toList()),
                      OutboxSqlArrays.instants(rows.stream().map(r -> r.stamp).toList()),
                      clock.now()));
      log.info(
          "Outbox gave {} queued rows back ({} released) - every slot is busy with a send older"
              + " than {}; any pod with a free slot takes them",
          rows.size(),
          released,
          OutboxConcurrentLane.HORIZON);
    } catch (RuntimeException ex) {
      log.warn(
          "Outbox could not give {} queued rows back - their leases lapse, unsent: {}",
          rows.size(),
          ex.toString());
    }
  }

  /** Whether a queued row has waited a tenth of its lease or longer since it was leased. */
  private boolean waitedLong(Leased row) {
    return Duration.between(row.leasedAt, clock.now()).compareTo(row.leaseLength.dividedBy(10))
        >= 0;
  }

  /**
   * The attempt a lapsed lease stands for: the previous holder's send was under way when its
   * lease ran out - it may have been delivered (the idempotency key dedups), it was never booked.
   */
  private static Attempt lapsed(Leased row) {
    return new Attempt(
        null,
        new OutboxLeaseLapsedException(
            "the lease of the send before this claim lapsed unbooked (the send was under way:"
                + " its pod died, or it outran its bound) - counted as a failed attempt; row "
                + row.event.getId()));
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

  @SuppressWarnings("java:S1181") // an Error is a failed attempt; nothing leaves the thread (F-4)
  private void relayOrdered(List<Leased> ordered) {
    for (int i = 0; i < ordered.size(); i++) {
      Leased row = ordered.get(i);
      if (row.lost) {
        continue;
      }
      int index = i;
      try {
        book(tx, row, row.lapsedSending ? lapsed(row) : send(row), () -> renewNext(ordered, index));
      } catch (Throwable ex) { // nothing leaves the ORDERED lane's thread
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
      row.setSendingSince(null);
      afterCommit.add(() -> next.lost = true);
      log.info("Outbox row {} cancelled before its send - released unsent", row.getId());
      return false;
    }
    if (next.lapsedSending) {
      return true; // its lapse is booked first, nothing sent: the lease it has covers that
    }
    OffsetDateTime now = clock.now();
    OffsetDateTime renewed = leaseEnd(row, next.publisher, rowLane, now);
    row.setClaimedUntil(renewed);
    row.setNextAttemptOn(renewed);
    row.setSendingSince(now); // renewed for the send that starts right after this commit
    afterCommit.add(
        () -> {
          next.leasedAt = now;
          next.stamp = renewed;
          next.marked = true;
        });
    return true;
  }

  /** One send, its lease kept by the heartbeat while it runs; whatever it throws is a failure. */
  @SuppressWarnings("java:S1181") // an Error is a failed attempt; nothing leaves the thread (F-4)
  private Attempt send(Leased row) {
    heartbeat.started(row);
    try {
      return new Attempt(row.publisher.deliver(row.event), null);
    } catch (Throwable ex) { // an Error is a failed attempt too (1.9.0)
      return new Attempt(null, asFailure(ex));
    } finally {
      heartbeat.finished(row);
    }
  }

  // ---------------------------------------------------------------- booking

  /**
   * Books one attempt in its own short transaction; {@code alsoInTx} rides the same transaction
   * and returns what to apply once it committed. A refused RELAYED booking (a hook or listener
   * threw - its writes roll back with the stamp) is booked as an ordinary failure instead; a
   * failure booking refused too (the hook threw again) is booked once more WITHOUT the hook, so
   * the attempt counts and the row's budget ends it - never redelivered unbooked forever (1.9.0).
   */
  @SuppressWarnings("java:S1181") // an Error is a failed attempt; nothing leaves the thread (F-4)
  private void book(
      TransactionOperations via, Leased row, Attempt attempt, Supplier<Runnable> alsoInTx) {
    RuntimeException failure = attempt.failure();
    if (failure == null) {
      try {
        commit(via, () -> bookRelayed(row, attempt.result()), alsoInTx);
        return;
      } catch (Throwable refused) {
        failure = asFailure(refused);
      }
    }
    RuntimeException failed = failure;
    try {
      commit(via, () -> bookFailure(row, failed, true), alsoInTx);
    } catch (Throwable refusedAgain) {
      log.error(
          "Outbox row {} to {} - the failure booking was refused as well (its booking hook"
              + " threw); booked without the hook",
          row.event.getId(),
          row.event.getDestination(),
          refusedAgain);
      failed.addSuppressed(refusedAgain);
      commit(via, () -> bookFailure(row, failed, false), alsoInTx);
    }
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
        repository.bookRelayed(leased.event.getId(), leased.stamp, clock.now());
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

  private Runnable bookFailure(Leased leased, RuntimeException ex, boolean withHook) {
    Optional<OutboxEvent> held = repository.lockLeased(leased.event.getId(), leased.stamp);
    if (held.isEmpty()) {
      logLapsed(leased);
      return NOTHING;
    }
    OutboxEvent row = held.get();
    row.setClaimedUntil(null);
    row.setSendingSince(null);
    OutboxBooking booking;
    if (row.getCancelledOn() != null) {
      bookAttempt(row, ex);
      row.setNextAttemptOn(clock.now()); // no longer the lease end; never claimed again anyway
      booking = OutboxBooking.failed(Outcome.CANCELLED, ex, null);
      log.info(
          "Outbox row {} - send failed after it was cancelled; it retires cancelled: {}",
          row.getId(),
          row.getLastError());
    } else {
      booking = registerFailure(row, leased.publisher, ex);
    }
    if (withHook) {
      leased.publisher.onBooked(row, booking);
    }
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
    event.setNextAttemptOn(clock.now().plus(backoffFor(event, publisher, attempts)));
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
      event.setRelayedOn(clock.now());
      log.warn(
          "Outbox row {} to {} DROPPED after {} attempts by its publisher's policy: {}",
          event.getId(),
          event.getDestination(),
          event.getAttempts(),
          event.getLastError());
      return ExhaustionOutcome.DROP;
    }
    event.setParkedOn(clock.now());
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
