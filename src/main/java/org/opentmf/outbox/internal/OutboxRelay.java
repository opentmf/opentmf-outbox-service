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

/**
 * The relay driver - in-service, no extra deployable. Three triggers feed ONE single-threaded
 * executor (the ordering rule: one claimer at a time per pod - and the ORDERED lane's sends run
 * on it; the lease is the cross-pod guard): the after-commit nudge (normal path, milliseconds),
 * a freed CONCURRENT slot, and a fixed-delay sweep (default 5s - "timers are for the tail").
 * Nudges coalesce: while a pass is queued, another nudge adds nothing. Each pass drains: it
 * keeps claiming batches while full batches come back.
 */
@Slf4j
@RequiredArgsConstructor
class OutboxRelay {

  private final OutboxRelayWorker worker;
  private final OutboxProperties properties;
  private final OutboxConcurrentLane lane;
  private final AtomicBoolean passQueued = new AtomicBoolean();

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
    executor.scheduleWithFixedDelay(
        this::relayPass, sweepMillis, sweepMillis, TimeUnit.MILLISECONDS);
    lane.onRelease(this::poke); // a freed slot is room for the next CONCURRENT row
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

  /** One drain: claims batches until a non-full batch signals the backlog is empty. */
  void relayPass() {
    passQueued.set(false); // from here on a nudge queues the NEXT pass
    try {
      int claimed;
      do {
        claimed = worker.relayBatch();
      } while (claimed >= properties.getBatchSize());
    } catch (RuntimeException ex) {
      log.error("Outbox relay pass failed; the sweep will retry", ex);
    }
  }

  /**
   * Stops the relay thread (a pass in flight gets the shutdown grace to finish its batch), then the
   * CONCURRENT lane: sends in flight get the shutdown grace to finish and book; the rest book
   * nothing and their rows are redelivered once their leases lapse.
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
