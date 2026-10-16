package org.opentmf.outboxinvariants;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

/** The harness's timed waits: a receiver's latency, a hang, an outage, a sampler's beat. */
final class Waits {

  private Waits() {}

  /** Waits {@code duration}; an interrupt ends it with {@link InterruptedException}. */
  static void interruptibly(Duration duration) throws InterruptedException {
    long end = System.nanoTime() + duration.toNanos();
    for (long left = duration.toNanos(); left > 0; left = end - System.nanoTime()) {
      if (Thread.interrupted()) {
        throw new InterruptedException("interrupted while waiting " + duration);
      }
      LockSupport.parkNanos(left);
    }
  }

  /** Waits {@code duration} through every interrupt, then restores the flag - a deaf call. */
  static void deafly(Duration duration) {
    long end = System.nanoTime() + duration.toNanos();
    boolean interrupted = false;
    for (long left = duration.toNanos(); left > 0; left = end - System.nanoTime()) {
      interrupted |= Thread.interrupted(); // parkNanos returns at once while the flag is set
      LockSupport.parkNanos(left);
    }
    if (interrupted || Thread.interrupted()) {
      Thread.currentThread().interrupt();
    }
  }
}
