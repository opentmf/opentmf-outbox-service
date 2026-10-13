package org.opentmf.outbox.internal;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskRejectedException;

/**
 * The CONCURRENT lane (1.6.0): a HAND-OFF QUEUE in front of {@code max-in-flight} slots. The relay
 * claims rows AHEAD of the slots and hands them over in claim order; the permits bound the SENDS,
 * never the claim. Each permit is one worker - a thread that takes queued sends one after another
 * until the queue is empty, then returns its permit. Workers run on VIRTUAL threads when the
 * runtime is 21+ and the application enables {@code spring.threads.virtual.enabled}, on platform
 * threads otherwise (the library's bytecode stays on Java 17; Spring's executor reaches the virtual
 * threads at runtime). {@link SimpleAsyncTaskExecutor} starts one thread per worker and has no
 * bound of its own, so the permits are the thread bound as well.
 *
 * <p>The queue holds at most {@code capacity} rows (the relay's {@code batch-size}): the relay
 * claims only while the queue is at most half full, and then only up to the capacity - so a claim
 * is never smaller than half a batch while a backlog lasts, and the other half keeps the slots busy
 * while the claim runs. Crossing that half-way mark on the way down nudges the relay, and so does a
 * worker running out of work. A queued row's lease runs while it waits: the send renews it first
 * once it has waited a tenth of it (see {@code OutboxRelayWorker}).
 */
@Slf4j
class OutboxConcurrentLane {

  private final Semaphore permits;
  private final int capacity;
  private final int lowWater;
  private final SimpleAsyncTaskExecutor executor;
  private final boolean virtual;
  private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
  private final AtomicInteger queued = new AtomicInteger();
  private final AtomicReference<Runnable> onRoom = new AtomicReference<>(() -> {});
  private volatile boolean closed;

  OutboxConcurrentLane(int maxInFlight, int capacity, boolean virtual, Duration shutdownGrace) {
    this.permits = new Semaphore(maxInFlight);
    this.capacity = capacity;
    this.lowWater = capacity / 2;
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

  /** Called when the queue drains to half its capacity or a worker runs out of work. */
  void onRoom(Runnable callback) {
    onRoom.set(callback);
  }

  /**
   * How many rows the relay may claim now: none while the queue is more than half full, else up to
   * the capacity.
   */
  int room() {
    int waiting = queued.get();
    return waiting > lowWater ? 0 : capacity - waiting;
  }

  /** Rows claimed and waiting for a slot. */
  int queued() {
    return queued.get();
  }

  /** Free slots right now. */
  int available() {
    return permits.availablePermits();
  }

  /**
   * Queues one claimed send behind those already waiting and starts a worker if a slot is free. A
   * closed lane refuses it; the row's lease then lapses and a relay takes it again.
   *
   * @return whether the send was queued
   */
  boolean submit(Runnable send) {
    if (closed) {
      return false;
    }
    queued.incrementAndGet(); // first: a worker's poll never drives the count below zero
    queue.add(send);
    startWorkers();
    return true;
  }

  /** One worker per free slot while sends wait. */
  private void startWorkers() {
    while (!closed && queued.get() > 0 && permits.tryAcquire()) {
      try {
        executor.execute(this::work);
      } catch (TaskRejectedException ex) {
        permits.release();
        log.debug("Outbox send worker not started - the concurrent lane is closing", ex);
        return;
      }
    }
  }

  /**
   * A worker: sends one queued row after another, then returns its slot. A send queued between its
   * last empty poll and the release found no free slot - so it looks once more, and starts the
   * worker that send needs.
   */
  private void work() {
    try {
      for (Runnable send = poll(); send != null; send = poll()) {
        send.run(); // never throws - the relay worker catches and logs
      }
    } finally {
      permits.release();
      startWorkers();
      onRoom.get().run();
    }
  }

  private Runnable poll() {
    if (closed) {
      return null;
    }
    Runnable send = queue.poll();
    if (send != null && queued.decrementAndGet() == lowWater) {
      onRoom.get().run(); // half drained: time for the next claim
    }
    return send;
  }

  /**
   * Stops taking sends and waits up to the shutdown grace for those in flight to finish and book;
   * queued rows are dropped unsent, and they - like whatever is still running after the grace - are
   * redelivered once their leases lapse.
   */
  void close() {
    closed = true;
    int dropped = 0;
    while (queue.poll() != null) {
      dropped++;
    }
    queued.set(0);
    if (dropped > 0) {
      log.info("Outbox relay stopping - {} queued rows not sent; their leases lapse", dropped);
    }
    executor.close();
  }
}
