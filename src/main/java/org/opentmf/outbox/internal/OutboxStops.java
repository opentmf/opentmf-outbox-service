package org.opentmf.outbox.internal;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The bounded waits of a stopping relay (1.11.1, OUTBOX-SHUTDOWN-THREADS-BEFORE-DATASOURCE-1): each
 * waits until a DEADLINE on {@link System#nanoTime()}, shared by everything the relay stops, so the
 * whole stop is one grace plus one join - not a grace per executor in turn.
 */
final class OutboxStops {

  private OutboxStops() {}

  /** The nanoseconds left before {@code deadline}, never negative. */
  static long left(long deadline) {
    return Math.max(0, deadline - System.nanoTime());
  }

  /**
   * Waits until the executor has terminated or the deadline passed; whether it terminated. An
   * interrupt of the stopping thread ends the wait at once, its flag restored.
   */
  static boolean awaitUntil(ExecutorService executor, long deadline) {
    try {
      return executor.awaitTermination(left(deadline), TimeUnit.NANOSECONDS);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      return executor.isTerminated();
    }
  }
}
