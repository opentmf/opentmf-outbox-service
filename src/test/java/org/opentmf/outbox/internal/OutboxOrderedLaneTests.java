package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The ORDERED lane (1.7.1): a batch runs on the lane's one daemon thread, never the caller's; one
 * batch at a time - a second is refused while the first sends; the lane is idle again BEFORE it
 * nudges the relay, also after a failing batch; close lets the batch in flight finish within the
 * grace, interrupts it after, and refuses new batches.
 */
class OutboxOrderedLaneTests {

  private static void hold(CountDownLatch gate) {
    try {
      assertThat(gate.await(10, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  @Test
  void aBatch_runsOnTheLanesDaemonThread_andOneBatchAtATime() {
    OutboxOrderedLane lane = new OutboxOrderedLane(Duration.ofSeconds(5));
    CountDownLatch gate = new CountDownLatch(1);
    List<Thread> ran = new CopyOnWriteArrayList<>();

    assertThat(lane.busy()).isFalse();
    assertThat(
            lane.submit(
                () -> {
                  ran.add(Thread.currentThread());
                  hold(gate);
                }))
        .isTrue();
    await().atMost(Duration.ofSeconds(5)).until(() -> ran.size() == 1);
    assertThat(lane.busy()).isTrue();
    assertThat(lane.submit(() -> ran.add(Thread.currentThread()))).isFalse(); // one at a time

    gate.countDown();

    await().atMost(Duration.ofSeconds(5)).until(() -> !lane.busy());
    assertThat(ran)
        .singleElement()
        .satisfies(
            thread -> {
              assertThat(thread.getName()).isEqualTo("opentmf-outbox-ordered");
              assertThat(thread.isDaemon()).isTrue(); // never holds the JVM open
              assertThat(thread).isNotSameAs(Thread.currentThread());
            });
    assertThat(lane.submit(() -> ran.add(Thread.currentThread()))).isTrue(); // idle again
    await().atMost(Duration.ofSeconds(5)).until(() -> ran.size() == 2);
    lane.close();
  }

  @Test
  void theLaneIsIdle_beforeItNudges_onceAfterEveryBatch() {
    OutboxOrderedLane lane = new OutboxOrderedLane(Duration.ofSeconds(5));
    List<Boolean> busyWhenNudged = new CopyOnWriteArrayList<>();
    lane.onIdle(() -> busyWhenNudged.add(lane.busy()));

    assertThat(lane.submit(() -> {})).isTrue();
    await().atMost(Duration.ofSeconds(5)).until(() -> busyWhenNudged.size() == 1);
    assertThat(lane.submit(() -> {})).isTrue();
    await().atMost(Duration.ofSeconds(5)).until(() -> busyWhenNudged.size() == 2);

    assertThat(busyWhenNudged).containsExactly(false, false);
    lane.close();
  }

  @Test
  void aFailingBatch_stillFreesTheLane_andNudges() {
    OutboxOrderedLane lane = new OutboxOrderedLane(Duration.ofSeconds(5));
    AtomicInteger nudges = new AtomicInteger();
    lane.onIdle(nudges::incrementAndGet);

    assertThat(
            lane.submit(
                () -> {
                  throw new IllegalStateException("booking failed");
                }))
        .isTrue();

    await().atMost(Duration.ofSeconds(5)).until(() -> nudges.get() == 1);
    assertThat(lane.busy()).isFalse();
    AtomicBoolean next = new AtomicBoolean();
    assertThat(lane.submit(() -> next.set(true))).isTrue(); // the thread is still serving
    await().atMost(Duration.ofSeconds(5)).until(next::get);
    lane.close();
  }

  @Test
  void close_letsTheBatchInFlightFinishWithinTheGrace_andRefusesNewOnes() {
    OutboxOrderedLane lane = new OutboxOrderedLane(Duration.ofSeconds(5));
    CountDownLatch started = new CountDownLatch(1);
    AtomicBoolean finished = new AtomicBoolean();
    assertThat(
            lane.submit(
                () -> {
                  started.countDown();
                  try { // the receiver's response time
                    assertThat(new CountDownLatch(1).await(300, TimeUnit.MILLISECONDS)).isFalse();
                  } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                  }
                  finished.set(true);
                }))
        .isTrue();
    hold(started);

    lane.close(); // waits up to the grace

    assertThat(finished.get()).isTrue();
    assertThat(lane.busy()).isFalse();
    assertThat(lane.submit(() -> {})).isFalse(); // closed: refused
    assertThat(lane.busy()).isFalse(); // and the refusal leaves it idle
  }

  @Test
  void close_interruptsABatchThatOutlivesTheGrace() {
    OutboxOrderedLane lane = new OutboxOrderedLane(Duration.ofMillis(200));
    CountDownLatch started = new CountDownLatch(1);
    AtomicBoolean interrupted = new AtomicBoolean();
    assertThat(
            lane.submit(
                () -> {
                  started.countDown();
                  try {
                    new CountDownLatch(1).await(30, TimeUnit.SECONDS);
                  } catch (InterruptedException ex) {
                    interrupted.set(true);
                    Thread.currentThread().interrupt();
                  }
                }))
        .isTrue();
    hold(started);
    long before = System.nanoTime();

    lane.close();

    assertThat(Duration.ofNanos(System.nanoTime() - before)).isLessThan(Duration.ofSeconds(5));
    await().atMost(Duration.ofSeconds(5)).until(interrupted::get);
  }
}
