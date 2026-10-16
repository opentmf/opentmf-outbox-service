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
 * The relay driver - in-service, no extra deployable. Four triggers feed ONE single-threaded
 * executor (the ordering rule: one claimer at a time per pod; the lease is the cross-pod guard):
 * the after-commit nudge (normal path, milliseconds), room in the CONCURRENT lane's hand-off
 * queue, the ORDERED lane going idle, and a fixed-delay sweep (default 5s - "timers are for the
 * tail"). Nudges coalesce: while a pass is queued, another nudge adds nothing - so any number of
 * nudges during a pass make ONE follow-up pass. The relay thread only CLAIMS (1.8.0): each lane
 * sends on its own threads, so no send - however slow - delays the other lane's next claim. A
 * pass drains the CONCURRENT lane: it claims again while that claim comes back full and the
 * queue has room.
 *
 * <p>The ORDERED claim runs only when ORDERED rows may be due (1.6.0) AND the ORDERED lane is
 * idle (1.8.0 - one batch in flight per pod): after a row of that lane (or of no known lane) was
 * appended or unparked in this pod, after an ORDERED claim that came back full, on the first
 * pass, and on every sweep - which picks up what no nudge announces: an ORDERED retry come due, a
 * lapsed lease, a row another pod appended and could not relay. While a batch is sending, the
 * flag waits; the lane nudges the relay when the batch is done. A CONCURRENT-only deployment thus
 * pays one claim per pass, not two.
 */
@Slf4j
@RequiredArgsConstructor
class OutboxRelay {

  private final OutboxRelayWorker worker;
  private final OutboxProperties properties;
  private final OutboxConcurrentLane lane;
  private final OutboxOrderedLane orderedLane;
  private final OutboxSendHeartbeat heartbeat;
  private final OutboxClock clock;
  private final AtomicBoolean passQueued = new AtomicBoolean();
  private final AtomicBoolean orderedDue = new AtomicBoolean(true);

  private ScheduledExecutorService executor;

  /**
   * Starts the single relay thread and schedules the sweep - after measuring the clock against
   * the database (1.9.0), so no claim ever runs on the pod's own clock when the database answers -
   * and the send heartbeat.
   */
  @PostConstruct
  void start() {
    clock.measure();
    heartbeat.onBeat(worker::giveBackStranded); // a stranded hand-off queue goes back, 1.9.0
    heartbeat.start();
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
    orderedLane.onIdle(this::poke); // an ORDERED batch is done: the next one may be claimed
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

  /**
   * One drain: claims the ORDERED lane when its rows may be due and it is idle, and the CONCURRENT
   * lane until its claim comes back short or its queue is full.
   */
  @SuppressWarnings("java:S1181") // an Error is a failed attempt; nothing leaves the thread (F-4)
  void relayPass() {
    passQueued.set(false); // from here on a nudge queues the NEXT pass
    try {
      boolean again;
      do {
        // only the relay thread starts a batch: the lane seen idle here is still idle at the claim
        boolean withOrdered = !orderedLane.busy() && orderedDue.getAndSet(false);
        Batch batch = worker.relayBatch(withOrdered);
        if (batch.orderedFull()) {
          orderedDue.set(true); // more ORDERED rows may be due - claimed once this batch is sent
        }
        again = batch.concurrentFull() && lane.room() > 0;
      } while (again);
    } catch (Throwable ex) { // anything: a periodic task that throws is never run again
      orderedDue.set(true); // the failed claim may have been the ORDERED one
      log.error("Outbox relay pass failed; the sweep will retry", ex);
    }
  }

  /**
   * Stops the relay thread (a pass in flight gets the shutdown grace to finish its claims), then
   * the ORDERED lane (its batch gets the grace to finish) and the CONCURRENT lane (sends in flight
   * get the grace to finish and book); queued rows and the rest book nothing and are redelivered
   * once their leases lapse.
   */
  @PreDestroy
  void stop() {
    if (executor == null) {
      orderedLane.close();
      lane.close();
      heartbeat.stop();
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
    orderedLane.close();
    lane.close();
    heartbeat.stop(); // last: the sends of the graceful drain keep their leases
  }
}
