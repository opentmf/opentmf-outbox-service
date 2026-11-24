package org.opentmf.outbox.internal;

import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.internal.OutboxRelayWorker.Batch;
import org.springframework.context.SmartLifecycle;

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
 *
 * <p>Lifecycle (1.12.0, OUTBOX-SHUTDOWN-THREADS-BEFORE-DATASOURCE-1): a {@link SmartLifecycle} in
 * phase {@link #PHASE}, so it starts when the context starts and stops in the context's lifecycle
 * stop, BEFORE any bean is destroyed - the DataSource among them. The phase lies after the web
 * server's stop ({@code DEFAULT_PHASE - 2048}: nothing new is appended over HTTP while the relay
 * stops) and before Spring Kafka's producer factory's ({@code Integer.MIN_VALUE}: the producers
 * are still open while the sends drain). Up to 1.11.0 it started in its {@code @PostConstruct} and
 * stopped in its {@code @PreDestroy} without waiting for its threads: a send woken by the stop
 * ran on after the context had closed, and a heartbeat tick in flight borrowed a connection while
 * the pool closed. {@code
 * @PreDestroy} remains as the safety net of a context that never ran its lifecycle stop.
 *
 * <p>A stop is not the end (OUTBOX-RELAY-RESTARTS-AFTER-A-STOP-1): a {@code start} after a {@code
 * stop} runs the relay again on new threads - the context's {@code stop()}/{@code start()}, and
 * Spring's test-context cache pausing a cached context on a switch and restarting it on reuse. It
 * restarts only once every thread of the previous run has ended; while one still runs (a blocking
 * call that ignored the stop's interrupt), the start is refused with an ERROR naming it, so no send
 * of the old run returns into the new one. The bean's destruction is the end: a start after it is
 * refused with an ERROR. {@code opentmf.outbox.relay.running} reads 1 while the relay runs.
 */
@Slf4j
@RequiredArgsConstructor
class OutboxRelay implements SmartLifecycle {

  /**
   * After the web server ({@code DEFAULT_PHASE - 2048}), before the Kafka producers ({@code
   * Integer.MIN_VALUE}) - the library cannot name the web server's class, which is optional.
   */
  static final int PHASE = DEFAULT_PHASE - 4096;

  private static final String RELAY_THREAD = "opentmf-outbox-relay";

  private final OutboxRelayWorker worker;
  private final OutboxProperties properties;
  private final OutboxConcurrentLane lane;
  private final OutboxOrderedLane orderedLane;
  private final OutboxSendHeartbeat heartbeat;
  private final OutboxClock clock;
  private final AtomicBoolean passQueued = new AtomicBoolean();
  private final AtomicBoolean orderedDue = new AtomicBoolean(true);

  private final AtomicBoolean beatHooked = new AtomicBoolean();

  /** NEW until the first start or stop; STOPPED between runs; DESTROYED for good. */
  private enum State {
    NEW,
    RUNNING,
    STOPPED,
    DESTROYED
  }

  private final AtomicReference<ScheduledExecutorService> executor = new AtomicReference<>();
  private volatile State state = State.NEW;

  /**
   * Starts the single relay thread and schedules the sweep - after measuring the clock against the
   * database (1.9.0), so no claim ever runs on the pod's own clock when the database answers - and
   * the send heartbeat. After a stop it opens the lanes and the worker again, on new threads, once
   * every thread of the previous run has ended; refused, with an ERROR, while one still runs and
   * after the bean's destruction.
   */
  @Override
  public synchronized void start() {
    switch (state) {
      case RUNNING -> {
        return;
      }
      case DESTROYED -> {
        log.error(
            "Outbox relay NOT started - it was destroyed; nothing is relayed by this context");
        return;
      }
      case STOPPED -> {
        List<String> alive = alive();
        if (!alive.isEmpty()) {
          log.error(
              "Outbox relay NOT restarted - {} of its previous run still running (a blocking call"
                  + " that ignored the stop's interrupt); nothing is relayed by this pod until a"
                  + " later start finds them ended",
              alive);
          return;
        }
        worker.open();
        orderedLane.open();
        lane.open();
        passQueued.set(false);
        orderedDue.set(true); // a new run's first pass claims the ORDERED lane too
      }
      case NEW -> {
        // the first run: the lanes and the worker are open from their construction
      }
    }
    state = State.RUNNING;
    clock.measure();
    if (beatHooked.compareAndSet(false, true)) {
      heartbeat.onBeat(worker::giveBackStranded); // a stranded hand-off queue goes back, 1.9.0
    }
    heartbeat.start();
    ScheduledExecutorService started =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, RELAY_THREAD);
              thread.setDaemon(true);
              return thread;
            });
    long sweepMillis = properties.getSweepInterval().toMillis();
    started.scheduleWithFixedDelay(this::sweep, sweepMillis, sweepMillis, TimeUnit.MILLISECONDS);
    executor.set(started);
    lane.onRoom(this::poke); // room in the hand-off queue: time for the next CONCURRENT claim
    orderedLane.onIdle(this::poke); // an ORDERED batch is done: the next one may be claimed
  }

  /** Enqueues an immediate relay pass (the after-commit nudge); safe to call anytime. */
  public void poke() {
    ScheduledExecutorService current = executor.get();
    if (current == null) {
      log.debug("Outbox relay poke ignored - relay not started");
      return;
    }
    if (!passQueued.compareAndSet(false, true)) {
      return; // a queued pass will see this row too
    }
    try {
      current.execute(this::relayPass);
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

  @Override
  public boolean isRunning() {
    return state == State.RUNNING;
  }

  @Override
  public int getPhase() {
    return PHASE;
  }

  /**
   * Stops the relay on ONE deadline (1.12.0): it takes no work from now on - no claim, no batch,
   * no send - and gives the claims, the ORDERED batch and the sends in flight the shutdown grace,
   * together, to finish and book. Then nothing more is sent or booked (a send that returns later
   * books nothing; its row is redelivered once its lease lapses), what still runs is interrupted,
   * and the stop waits for every thread of the relay to END - the heartbeat's tick in flight
   * included, the heartbeat stopping last so the drained sends keep their leases - up to {@code
   * shutdown-join}. A thread still running then is named in one warning. At most {@code
   * shutdown-grace + shutdown-join} in all; once per run - a stop of a stopped relay returns at
   * once, the {@code @PreDestroy} safety net after the lifecycle stop among them.
   */
  @Override
  public synchronized void stop() {
    if (state == State.STOPPED || state == State.DESTROYED) {
      return;
    }
    state = State.STOPPED;
    ScheduledExecutorService current = executor.get();
    long graceEnd = System.nanoTime() + properties.getShutdownGrace().toNanos();
    if (current != null) {
      current.shutdown();
    }
    orderedLane.shutdown();
    lane.shutdown();
    awaitAll(graceEnd);
    worker.close();
    if (current != null) {
      current.shutdownNow();
    }
    orderedLane.interrupt();
    lane.interrupt();
    long joinEnd = System.nanoTime() + properties.getShutdownJoin().toNanos();
    awaitAll(joinEnd);
    List<String> alive = new ArrayList<>(relayThreadAlive());
    alive.addAll(orderedLane.alive());
    alive.addAll(lane.alive());
    alive.addAll(heartbeat.stopUntil(joinEnd)); // last: the drained sends kept their leases
    if (!alive.isEmpty()) {
      log.warn(
          "Outbox relay stopped with {} still running after the shutdown grace ({}) and join ({})"
              + " - a blocking call that ignores interrupts; its rows are redelivered once their"
              + " leases lapse",
          alive,
          properties.getShutdownGrace(),
          properties.getShutdownJoin());
    }
  }

  /**
   * The bean's destruction: the stop, if the context never ran its lifecycle stop - and the end; a
   * start from now on is refused.
   */
  @PreDestroy
  synchronized void destroy() {
    stop();
    state = State.DESTROYED;
  }

  /** The threads of the relay still running: the relay thread, both lanes', the heartbeat's. */
  private List<String> alive() {
    List<String> alive = new ArrayList<>(relayThreadAlive());
    alive.addAll(orderedLane.alive());
    alive.addAll(lane.alive());
    alive.addAll(heartbeat.alive());
    return alive;
  }

  /** The relay thread, while it still runs. */
  private List<String> relayThreadAlive() {
    ScheduledExecutorService current = executor.get();
    return current == null || current.isTerminated() ? List.of() : List.of(RELAY_THREAD);
  }

  /** Waits for the relay thread and both lanes until the deadline. */
  private void awaitAll(long deadline) {
    ScheduledExecutorService current = executor.get();
    if (current != null) {
      OutboxStops.awaitUntil(current, deadline);
    }
    orderedLane.awaitUntil(deadline);
    lane.awaitUntil(deadline);
  }
}
