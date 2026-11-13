package org.opentmf.outbox.internal;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A call that takes its time and ignores interrupts, as a JDBC socket read does: the stimulus of
 * the stop cells (1.11.1) - a thread that an interrupt does not end at once, so only a stop that
 * waits for it sees it end. A timed wait on a latch nobody opens; the interrupt flag is restored.
 */
final class UninterruptibleHold {

  private UninterruptibleHold() {}

  static void hold(Duration time) {
    CountDownLatch never = new CountDownLatch(1);
    long end = System.nanoTime() + time.toNanos();
    boolean interrupted = false;
    for (long left = time.toNanos(); left > 0; left = end - System.nanoTime()) {
      try {
        if (never.await(left, TimeUnit.NANOSECONDS)) {
          break;
        }
      } catch (InterruptedException ex) {
        interrupted = true; // ignored, as a blocking read ignores it
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
