package org.opentmf.outbox.internal;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
 * <p>HOW FAR AHEAD the relay claims follows the sends (1.8.1): the queue holds no more rows than
 * the slots start within {@link #HORIZON} - {@code max-in-flight x 1 s / send time}, from a moving
 * average of the sends, and never more than {@code capacity} (the relay's {@code batch-size}). With
 * fast sends that is the 1.6.0 queue, and the claim's cost stays off the sends' path; with sends
 * slower than a second per slot it is NONE: the claim takes only the free slots, and no row waits
 * behind a busy slot while its lease runs out. A queued row thus waits about a second at most.
 * Until a send has been measured the lane claims nothing ahead. The relay claims only
 * while the queue is at most half that depth; crossing it on the way down nudges the relay, and so
 * does a worker running out of work. Up to 1.8.0 the depth was always the capacity: behind slow
 * sends a short-lease row waited in the queue, its lease lapsed, the next pass claimed it again and
 * queued it at the tail - for minutes.
 *
 * <p>A queued row's lease still runs while it waits: the send renews it first once it has waited
 * a tenth of it, and the relay never claims a row it still holds queued (see {@code
 * OutboxRelayWorker}).
 */
@Slf4j
class OutboxConcurrentLane {

  /** The moving average's weight of a new sample: 1/8. */
  private static final int SMOOTHING = 8;

  /** How long a queued row may wait for a slot, at most about: the queue's depth follows it. */
  static final Duration HORIZON = Duration.ofSeconds(1);

  private final Semaphore permits;
  private final int maxInFlight;
  private final int capacity;
  private final SimpleAsyncTaskExecutor executor;
  private final boolean virtual;
  private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
  private final AtomicInteger queued = new AtomicInteger();
  private final AtomicReference<Runnable> onRoom = new AtomicReference<>(() -> {});
  /** Moving average of one send (renewal, send, booking), in nanoseconds; 0 = none measured yet. */
  private final AtomicLong sendNanos = new AtomicLong();
  private volatile boolean closed;

  OutboxConcurrentLane(int maxInFlight, int capacity, boolean virtual, Duration shutdownGrace) {
    this.permits = new Semaphore(maxInFlight);
    this.maxInFlight = maxInFlight;
    this.capacity = capacity;
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

  /** Called when the queue drains to half its depth or a worker runs out of work. */
  void onRoom(Runnable callback) {
    onRoom.set(callback);
  }

  /** One send (its renewal, the send, its booking) took this long. */
  void sendTook(long nanos) {
    long sample = Math.max(1, nanos);
    sendNanos.updateAndGet(old -> old == 0 ? sample : old + (sample - old) / SMOOTHING);
  }

  /**
   * How many rows the queue holds ahead of the slots: as many as the slots start within the
   * {@link #HORIZON}, at most the capacity; none before a send has been measured, and none while a
   * send takes longer than the horizon per slot - a queued row would only wait behind them.
   */
  int depth() {
    long send = sendNanos.get();
    if (send == 0) {
      return 0;
    }
    return (int) Math.min(capacity, maxInFlight * HORIZON.toNanos() / send);
  }

  /**
   * How many rows the relay may claim now: none while the queue is more than half its depth, else
   * the free slots plus the depth, less what is queued - at most the capacity.
   */
  int room() {
    int waiting = queued.get();
    int depth = depth();
    if (waiting > depth / 2) {
      return 0;
    }
    return Math.max(0, Math.min(capacity, permits.availablePermits() + depth) - waiting);
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
        long started = System.nanoTime();
        send.run(); // never throws - the relay worker catches and logs
        sendTook(System.nanoTime() - started);
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
    if (send != null && queued.decrementAndGet() == depth() / 2) {
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
