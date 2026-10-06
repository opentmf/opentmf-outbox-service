package org.opentmf.outbox.internal;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskRejectedException;

/**
 * The CONCURRENT lane's slots and threads: one send per thread - VIRTUAL when the runtime is 21+
 * and the application enables {@code spring.threads.virtual.enabled}, platform threads
 * otherwise (the library's bytecode stays on Java 17; Spring's executor reaches the virtual
 * threads at runtime). {@code max-in-flight} permits bound the sends.
 *
 * <p>The permit is taken by the CLAIM, before the row is stamped and long before the task is
 * submitted - never inside the task. That is what makes the platform-thread fallback safe:
 * {@link SimpleAsyncTaskExecutor} starts one thread per task and has no bound of its own, so the
 * permits are the only thing that limits how many threads exist. The task returns its permit when
 * its booking is done, and the release nudges the relay to fill the slot.
 */
@Slf4j
class OutboxConcurrentLane {

  private final Semaphore permits;
  private final SimpleAsyncTaskExecutor executor;
  private final boolean virtual;
  private final AtomicReference<Runnable> onRelease = new AtomicReference<>(() -> {});

  OutboxConcurrentLane(int maxInFlight, boolean virtual, Duration shutdownGrace) {
    this.permits = new Semaphore(maxInFlight);
    this.virtual = virtual;
    this.executor = new SimpleAsyncTaskExecutor("opentmf-outbox-send-");
    executor.setDaemon(true);
    executor.setVirtualThreads(virtual);
    executor.setTaskTerminationTimeout(shutdownGrace.toMillis());
  }

  /** Whether the sends run on virtual threads. */
  boolean isVirtual() {
    return virtual;
  }

  /** Called after every released permit - the relay's nudge. */
  void onRelease(Runnable callback) {
    onRelease.set(callback);
  }

  /** Takes one slot if one is free; the claim calls this BEFORE it stamps a CONCURRENT row. */
  boolean tryAcquire() {
    return permits.tryAcquire();
  }

  /** Returns one slot and nudges the relay. */
  void release() {
    permits.release();
    onRelease.get().run();
  }

  /** Returns slots taken by a claim that did not commit - no nudge, nothing was sent. */
  void releaseUnused(int count) {
    permits.release(count);
  }

  /** Free slots right now. */
  int available() {
    return permits.availablePermits();
  }

  /**
   * Starts a send whose slot the claim already holds. A rejected start (the lane is closing)
   * returns the slot; the row's lease then lapses and another relay takes it.
   *
   * @return whether the send started
   */
  boolean submit(Runnable send) {
    try {
      executor.execute(send);
      return true;
    } catch (TaskRejectedException ex) {
      log.debug("Outbox send not started - the concurrent lane is closing", ex);
      permits.release();
      return false;
    }
  }

  /**
   * Stops taking sends and waits up to the shutdown grace for those in flight to finish and
   * book; whatever is still running after that books nothing, and its lease lapses.
   */
  void close() {
    executor.close();
  }
}
