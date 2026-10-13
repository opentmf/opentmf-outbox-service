package org.opentmf.outbox.internal;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.internal.OutboxRelayWorker.Batch;

/**
 * The relay driver - in-service, no extra deployable. Three triggers feed ONE single-threaded
 * executor (the ordering rule: one claimer at a time per pod - and the ORDERED lane's sends run
 * on it; the lease is the cross-pod guard): the after-commit nudge (normal path, milliseconds),
 * room in the CONCURRENT lane's hand-off queue, and a fixed-delay sweep (default 5s - "timers are
 * for the tail"). Nudges coalesce: while a pass is queued, another nudge adds nothing - so any
 * number of nudges during a pass make ONE follow-up pass. Each pass drains: it keeps claiming
 * while a lane's claim comes back full (the CONCURRENT lane only while its queue has room).
 *
 * <p>The ORDERED claim runs only when ORDERED rows may be due (1.6.0): after a row of that lane
 * (or of no known lane) was appended or unparked in this pod, after an ORDERED claim that came
 * back full, on the first pass, and on every sweep - which picks up what no nudge announces: an
 * ORDERED retry come due, a lapsed lease, a row another pod appended and could not relay. A
 * CONCURRENT-only deployment thus pays one claim per pass, not two.
 */
@Slf4j
@RequiredArgsConstructor
class OutboxRelay {

  private final OutboxRelayWorker worker;
  private final OutboxProperties properties;
  private final OutboxConcurrentLane lane;
  private final AtomicBoolean passQueued = new AtomicBoolean();
  private final AtomicBoolean orderedDue = new AtomicBoolean(true);

  private ScheduledExecutorService executor;

  /** Starts the single relay thread and schedules the sweep. */
  @PostConstruct
  void start() {
    executor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "opentmf-outbox-relay");
              thread.setDaemon(true);
              return thread;
            });
    long sweepMillis = properties.getSweepInterval().toMillis();
    executor.scheduleWithFixedDelay(this::sweep, sweepMillis, sweepMillis, TimeUnit.MILLISECONDS);
    lane.onRoom(this::poke); // room in the hand-off queue: time for the next CONCURRENT claim
  }

  /** Enqueues an immediate relay pass (the after-commit nudge); safe to call anytime. */
  public void poke() {
    if (executor == null) {
      log.debug("Outbox relay poke ignored - relay not started");
      return;
    }
    if (!passQueued.compareAndSet(false, true)) {
      return; // a queued pass will see this row too
    }
    try {
      executor.execute(this::relayPass);
    } catch (RejectedExecutionException ex) {
      passQueued.set(false);
      log.debug("Outbox relay poke ignored - relay is shut down", ex);
    }
  }

  /** The nudge for a row the ORDERED lane carries (or of no known lane): its claim runs too. */
  public void pokeOrdered() {
    orderedDue.set(true);
    poke();
  }

  /** The sweep: a pass that claims the ORDERED lane whatever the nudges said. */
  void sweep() {
    orderedDue.set(true);
    relayPass();
  }

  /** One drain: claims batches until no lane's claim comes back full. */
  void relayPass() {
    passQueued.set(false); // from here on a nudge queues the NEXT pass
    try {
      boolean again;
      do {
        Batch batch = worker.relayBatch(orderedDue.getAndSet(false));
        if (batch.orderedFull()) {
          orderedDue.set(true); // more ORDERED rows may be due
        }
        again = batch.orderedFull() || (batch.concurrentFull() && lane.room() > 0);
      } while (again);
    } catch (RuntimeException ex) {
      orderedDue.set(true); // the failed claim may have been the ORDERED one
      log.error("Outbox relay pass failed; the sweep will retry", ex);
    }
  }

  /**
   * Stops the relay thread (a pass in flight gets the shutdown grace to finish its batch), then the
   * CONCURRENT lane: sends in flight get the shutdown grace to finish and book; queued rows and the
   * rest book nothing and are redelivered once their leases lapse.
   */
  @PreDestroy
  void stop() {
    if (executor == null) {
      lane.close();
      return;
    }
    executor.shutdown();
    try {
      if (!executor.awaitTermination(
          properties.getShutdownGrace().toMillis(), TimeUnit.MILLISECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
      executor.shutdownNow();
    }
    lane.close();
  }
}
