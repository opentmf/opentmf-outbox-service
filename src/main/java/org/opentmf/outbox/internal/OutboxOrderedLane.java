package org.opentmf.outbox.internal;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * The ORDERED lane (1.8.0): ONE daemon thread that sends a claimed ORDERED batch row after row, in
 * claim order - off the relay thread, which from now on only claims. Up to 1.7.0 the relay thread
 * sent the batch itself, so a slow receiver on the ORDERED lane (a hundred rows at a second each)
 * held back the CONCURRENT lane's next claim for the whole batch.
 *
 * <p>At most ONE batch is in flight per pod: the relay claims the ORDERED lane only while this lane
 * is idle ({@link #busy()}), a second batch is refused, and the lane nudges the relay when a batch
 * is done ({@link #onIdle}). The ORDERED rows of a pod therefore still go one at a time, in {@code
 * id} order, each under its own short lease renewed by the booking of the row before it.
 */
@Slf4j
class OutboxOrderedLane {

  private final ExecutorService executor;
  private final Duration shutdownGrace;
  private final AtomicBoolean busy = new AtomicBoolean();
  private final AtomicReference<Runnable> onIdle = new AtomicReference<>(() -> {});

  OutboxOrderedLane(Duration shutdownGrace) {
    this.shutdownGrace = shutdownGrace;
    this.executor =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "opentmf-outbox-ordered");
              thread.setDaemon(true);
              return thread;
            });
  }

  /** Called after each batch, once the lane is idle again: time for the next ORDERED claim. */
  void onIdle(Runnable callback) {
    onIdle.set(callback);
  }

  /** Whether a batch is in flight - the relay claims the ORDERED lane only while it is not. */
  boolean busy() {
    return busy.get();
  }

  /**
   * Hands one claimed batch to the lane's thread. Refused while another batch is in flight and once
   * the lane is closed; the batch's leases then lapse and a relay takes its rows again.
   *
   * @return whether the batch was taken
   */
  boolean submit(Runnable batch) {
    if (!busy.compareAndSet(false, true)) {
      return false;
    }
    try {
      executor.execute(() -> run(batch));
      return true;
    } catch (RejectedExecutionException ex) {
      busy.set(false);
      log.debug("Outbox ORDERED batch not started - the ordered lane is closed", ex);
      return false;
    }
  }

  @SuppressWarnings("java:S1181") // an Error is a failed attempt; nothing leaves the thread (F-4)
  private void run(Runnable batch) {
    try {
      batch.run(); // books and logs each row's outcome itself
    } catch (Throwable ex) { // nothing leaves the lane's thread (1.9.0: an Error killed it)
      log.error(
          "Outbox ORDERED batch failed; its unsent rows are redelivered once their leases lapse",
          ex);
    } finally {
      busy.set(false); // first: the nudged pass must see the lane idle
      onIdle.get().run();
    }
  }

  /**
   * Stops taking batches and gives the batch in flight the shutdown grace to finish and book; the
   * rows it has not sent by then are redelivered once their leases lapse.
   */
  void close() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(shutdownGrace.toMillis(), TimeUnit.MILLISECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
      executor.shutdownNow();
    }
  }
}
