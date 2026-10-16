package org.opentmf.outbox.internal;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.opentmf.outbox.internal.OutboxRelayWorker.Leased;
import org.springframework.transaction.support.TransactionOperations;

/**
 * The SEND HEARTBEAT (1.9.0): while a pod is SENDING a row, it keeps the row's lease. Every {@link
 * #TICK} one daemon thread ({@code opentmf-outbox-heartbeat}) looks at the sends in flight and, for
 * each whose lease has run a third of its length, renews it to {@code now + lease} - ONE guarded
 * statement for every renewal due ({@link OutboxClaimSql#RENEW_SENDING}) - and marks the send as
 * under way ({@code sending_since}). A long send of a live pod is thus never taken over; a pod that
 * dies stops renewing and its leases lapse as before.
 *
 * <p>The renewals are BOUNDED: a send is renewed up to its {@code send bound} - the larger of its
 * lease and its send timeout (the publisher's own, else {@code send-timeout}) - after its start,
 * never further. A send still running then is presumed hung: the heartbeat interrupts its thread
 * (once, best effort - a blocking call that ignores interrupts keeps its thread until it returns),
 * stops renewing, and the lease lapses; the next claim counts the lapse as a failed attempt, so the
 * row's budget ends it. Up to 1.8.0 nothing renewed a running send: a send longer than its lease
 * was taken over, sent again, never counted - every lease, forever - and its ORDERED lane or its
 * key stalled behind it.
 *
 * <p>A renewal and the send's own booking never race: both take the row's lock, the booking only
 * once the send is done, the heartbeat only while it runs, and the booking then guards on the stamp
 * the heartbeat left. The heartbeat thread also keeps the {@link OutboxClock}'s offset measured.
 */
@Slf4j
class OutboxSendHeartbeat {

  /** How often the sends in flight are looked at - a fraction of the shortest sensible lease. */
  static final Duration TICK = Duration.ofMillis(200);

  private final OutboxEventRepository repository;
  private final TransactionOperations tx;
  private final OutboxClock clock;
  private final Duration sendTimeout;
  private final Set<Leased> sending = ConcurrentHashMap.newKeySet();
  private final List<Runnable> onBeat = new CopyOnWriteArrayList<>();
  private ScheduledExecutorService executor;

  OutboxSendHeartbeat(
      OutboxEventRepository repository,
      TransactionOperations tx,
      OutboxClock clock,
      Duration sendTimeout) {
    this.repository = repository;
    this.tx = tx;
    this.clock = clock;
    this.sendTimeout = sendTimeout;
  }

  /** Starts the heartbeat thread. */
  void start() {
    executor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "opentmf-outbox-heartbeat");
              thread.setDaemon(true);
              return thread;
            });
    executor.scheduleWithFixedDelay(
        this::beat, TICK.toMillis(), TICK.toMillis(), TimeUnit.MILLISECONDS);
  }

  /** Stops it - after the lanes, so the sends of a graceful stop keep their leases. */
  void stop() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  /**
   * The longest a send runs before it is presumed hung: its lease or its send timeout - the
   * publisher's own ({@code OutboxPublisher.sendTimeout}), else {@code send-timeout} - the larger.
   */
  Duration bound(Leased row) {
    Duration declared = row.publisher.sendTimeout(row.event);
    Duration timeout =
        declared != null && !declared.isNegative() && !declared.isZero() ? declared : sendTimeout;
    return row.leaseLength.compareTo(timeout) >= 0 ? row.leaseLength : timeout;
  }

  /** A send starts on the calling thread: from now on its lease is kept, up to its bound. */
  void started(Leased row) {
    row.lock.lock();
    try {
      row.sendStartedAt = clock.instant();
      row.sender = Thread.currentThread();
      row.sending = true;
    } finally {
      row.lock.unlock();
    }
    sending.add(row);
  }

  /**
   * The send returned (or threw): the heartbeat leaves its lease alone from here on. Clears the
   * heartbeat's own interrupt, never anyone else's.
   */
  void finished(Leased row) {
    boolean interrupted;
    row.lock.lock();
    try {
      row.sending = false;
      interrupted = row.interrupted;
    } finally {
      row.lock.unlock();
    }
    sending.remove(row);
    if (interrupted) {
      Thread.interrupted(); // ours: the bound ran out - the booking must not inherit it
    }
  }

  /** Runs {@code task} on every tick too, after the renewals (the lane's give-back, 1.9.0). */
  void onBeat(Runnable task) {
    onBeat.add(task);
  }

  /** One tick, on the heartbeat thread; never throws - a failure waits for the next tick. */
  void beat() {
    try {
      clock.measureIfDue();
      renewDue();
      onBeat.forEach(Runnable::run);
    } catch (RuntimeException ex) {
      log.warn("Outbox send heartbeat failed - the next tick tries again", ex);
    }
  }

  /** One renewal due: the row, held under its lock until the statement is done. */
  private record Renewal(Leased row, OffsetDateTime renewed) {}

  private void renewDue() {
    Instant now = clock.instant();
    List<Renewal> due = new ArrayList<>();
    try {
      for (Leased row : sending) {
        Renewal renewal = dueRenewal(row, now);
        if (renewal != null) {
          due.add(renewal); // its lock stays held until the statement is done
        }
      }
      if (!due.isEmpty()) {
        renew(due);
      }
    } finally {
      due.forEach(renewal -> renewal.row().lock.unlock());
    }
  }

  /**
   * The renewal one send is due, with its lock HELD - or null (the lock released): the send is done
   * or its booking holds the lock, its lease still has two thirds to run, or it passed its bound
   * (then it is interrupted, once, and renewed no more).
   */
  private Renewal dueRenewal(Leased row, Instant now) {
    if (!row.lock.tryLock()) {
      return null; // its booking is under way
    }
    Renewal renewal = null;
    try {
      if (!row.sending || row.lost) {
        return null;
      }
      Instant deadline = row.sendStartedAt.plus(bound(row));
      if (!now.isBefore(deadline)) {
        interruptOnce(row);
        return null;
      }
      Instant stamp = row.stamp.toInstant();
      boolean thirdRun =
          Duration.between(now, stamp).compareTo(row.leaseLength.multipliedBy(2).dividedBy(3)) <= 0;
      if (!thirdRun) {
        return null;
      }
      Instant end = now.plus(row.leaseLength);
      Instant renewed = end.isBefore(deadline) ? end : deadline;
      if (renewed.isBefore(stamp)) {
        renewed = stamp; // never shorter: the mark alone
      }
      if (row.marked && !renewed.isAfter(stamp)) {
        return null; // marked, and at its bound already: nothing to write
      }
      renewal =
          new Renewal(
              row,
              OffsetDateTime.ofInstant(renewed, ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS));
      return renewal;
    } finally {
      if (renewal == null) {
        row.lock.unlock();
      }
    }
  }

  private void interruptOnce(Leased row) {
    if (row.interrupted) {
      return;
    }
    row.interrupted = true;
    log.warn(
        "Outbox row {} to {} - its send has run {} (its bound: the larger of its lease and"
            + " send-timeout); presumed hung: interrupted, its lease left to lapse - the next claim"
            + " counts the lapse as a failed attempt",
        row.event.getId(),
        row.event.getDestination(),
        bound(row));
    row.sender.interrupt();
  }

  private void renew(List<Renewal> due) {
    String ids = OutboxSqlArrays.ids(due.stream().map(r -> r.row().event.getId()).toList());
    String stamps = OutboxSqlArrays.instants(due.stream().map(r -> r.row().stamp).toList());
    String renewed = OutboxSqlArrays.instants(due.stream().map(Renewal::renewed).toList());
    String started =
        OutboxSqlArrays.instants(
            due.stream()
                .map(
                    r ->
                        OffsetDateTime.ofInstant(r.row().sendStartedAt, ZoneOffset.UTC)
                            .truncatedTo(ChronoUnit.MICROS))
                .toList());
    Set<Long> kept;
    try {
      kept =
          tx.execute(
              status -> new HashSet<>(repository.renewSending(ids, stamps, renewed, started)));
    } catch (RuntimeException ex) {
      log.warn(
          "Outbox send heartbeat could not renew {} leases - they run on; the next tick tries"
              + " again: {}",
          due.size(),
          ex.toString());
      return;
    }
    for (Renewal renewal : due) {
      Leased row = renewal.row();
      if (kept != null && kept.contains(row.event.getId())) {
        row.stamp = renewal.renewed();
        row.marked = true;
      } else {
        row.lost = true; // another holder took it, or a booking cleared it
      }
    }
  }
}
