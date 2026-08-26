package org.opentmf.outbox;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * The S23 relay driver - in-service, no extra deployable. Two triggers feed ONE single-threaded
 * executor (the S23 ordering rule: one claimer at a time per pod; {@code SKIP LOCKED} is the
 * cross-pod guard): the after-commit nudge (normal path, milliseconds) and a fixed-delay sweep
 * (default 5s - "timers are for the tail"). Each pass drains: it keeps claiming batches while
 * full batches come back.
 */
@Slf4j
@RequiredArgsConstructor
class OutboxRelay {

  private final OutboxRelayWorker worker;
  private final OutboxProperties properties;

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
  }

  /** Enqueues an immediate relay pass (the after-commit nudge); safe to call anytime. */
  public void poke() {
    if (executor == null) {
      log.debug("Outbox relay poke ignored - relay not started");
      return;
    }
    try {
      executor.execute(this::relayPass);
    } catch (RejectedExecutionException ex) {
      log.debug("Outbox relay poke ignored - relay is shut down", ex);
    }
  }

  /** One drain: claims batches until a non-full batch signals the backlog is empty. */
  void relayPass() {
    try {
      int claimed;
      do {
        claimed = worker.relayBatch();
      } while (claimed >= properties.getBatchSize());
    } catch (RuntimeException ex) {
      log.error("Outbox relay pass failed; the sweep will retry", ex);
    }
  }

  /** Stops the relay thread; a pass in flight gets a grace period to finish its batch. */
  @PreDestroy
  void stop() {
    if (executor == null) {
      return;
    }
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      executor.shutdownNow();
    }
  }
}
