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
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * The hand-off queue in front of the slots: the permits bound the sends while the queue holds the
 * rest; queued sends run in claim order; the room the relay may claim, and the nudge half-way down;
 * no queued send is ever stranded; threads are daemons; close drops the queue and waits up to the
 * grace for the sends in flight.
 */
class OutboxConcurrentLaneTests {

  private static void hold(CountDownLatch gate) {
    try {
      assertThat(gate.await(10, TimeUnit.SECONDS)).isTrue();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  @Test
  void thePermits_boundTheSends_theQueueHoldsTheRest() {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(2, 10, false, Duration.ofSeconds(5));
    CountDownLatch gate = new CountDownLatch(1);
    AtomicInteger running = new AtomicInteger();
    AtomicInteger most = new AtomicInteger();
    AtomicInteger done = new AtomicInteger();
    for (int i = 0; i < 6; i++) {
      assertThat(
              lane.submit(
                  () -> {
                    most.accumulateAndGet(running.incrementAndGet(), Math::max);
                    hold(gate);
                    running.decrementAndGet();
                    done.incrementAndGet();
                  }))
          .isTrue();
    }

    await().atMost(Duration.ofSeconds(5)).until(() -> running.get() == 2);
    assertThat(lane.available()).isZero();
    assertThat(lane.queued()).isEqualTo(4); // claimed ahead, waiting for a slot
    gate.countDown();
    await().atMost(Duration.ofSeconds(5)).until(() -> done.get() == 6);
    assertThat(most.get()).isEqualTo(2);
    await().atMost(Duration.ofSeconds(5)).until(() -> lane.available() == 2);
    assertThat(lane.queued()).isZero();
    lane.close();
  }

  @Test
  void queuedSends_runInClaimOrder() {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(1, 10, false, Duration.ofSeconds(5));
    List<Integer> sent = new CopyOnWriteArrayList<>();

    IntStream.range(0, 10).forEach(i -> lane.submit(() -> sent.add(i)));

    await().atMost(Duration.ofSeconds(5)).until(() -> sent.size() == 10);
    assertThat(sent).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    lane.close();
  }

  @Test
  void theRoom_isNoneAboveHalf_andTopsUpToTheCapacity_andHalfWayDownNudgesOnce() {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(1, 10, false, Duration.ofSeconds(5));
    AtomicInteger nudges = new AtomicInteger();
    lane.onRoom(nudges::incrementAndGet);
    CountDownLatch gate = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(1);
    assertThat(lane.room()).isEqualTo(10);

    lane.submit(
        () -> {
          started.countDown();
          hold(gate);
        });
    hold(started); // the one slot is busy, nothing queued
    IntStream.range(0, 5).forEach(i -> lane.submit(() -> {}));
    assertThat(lane.room()).isEqualTo(5); // half full: claim back up to the capacity
    lane.submit(() -> {});
    assertThat(lane.room()).isZero(); // above half: no claim
    assertThat(nudges.get()).isZero();

    gate.countDown();

    await().atMost(Duration.ofSeconds(5)).until(() -> lane.available() == 1);
    await() // once crossing half-way down, once when the worker ran dry - never per send
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> assertThat(nudges.get()).isEqualTo(2));
    assertThat(lane.room()).isEqualTo(10);
    lane.close();
  }

  @Test
  void noQueuedSend_isEverStranded_whileWorkersComeAndGo() {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(2, 100_000, false, Duration.ofSeconds(5));
    AtomicInteger done = new AtomicInteger();

    for (int i = 0; i < 50_000; i++) { // workers drain and exit while sends keep arriving
      lane.submit(done::incrementAndGet);
    }

    await().atMost(Duration.ofSeconds(30)).until(() -> done.get() == 50_000);
    await().atMost(Duration.ofSeconds(5)).until(() -> lane.available() == 2);
    assertThat(lane.queued()).isZero();
    lane.close();
  }

  @Test
  void aSend_runsOnADaemonLaneThread() throws Exception {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(1, 10, false, Duration.ofSeconds(1));
    AtomicBoolean daemon = new AtomicBoolean();
    CountDownLatch ran = new CountDownLatch(1);

    assertThat(
            lane.submit(
                () -> {
                  daemon.set(Thread.currentThread().isDaemon());
                  ran.countDown();
                }))
        .isTrue();

    assertThat(ran.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(daemon.get()).isTrue(); // never holds the JVM open
    assertThat(lane.isVirtual()).isFalse();
    lane.close();
  }

  @Test
  void close_dropsTheQueue_andLetsASendInFlightFinishWithinTheGrace() throws Exception {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(1, 10, false, Duration.ofSeconds(5));
    CountDownLatch started = new CountDownLatch(1);
    AtomicBoolean finished = new AtomicBoolean();
    AtomicInteger queuedRan = new AtomicInteger();
    lane.submit(
        () -> {
          started.countDown();
          try { // the send's own response time
            assertThat(new CountDownLatch(1).await(300, TimeUnit.MILLISECONDS)).isFalse();
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
          finished.set(true);
        });
    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
    IntStream.range(0, 3).forEach(i -> lane.submit(queuedRan::incrementAndGet));

    lane.close(); // waits up to the grace

    assertThat(finished.get()).isTrue();
    assertThat(queuedRan.get()).isZero(); // dropped unsent: their leases lapse
    assertThat(lane.queued()).isZero();
    assertThat(lane.submit(() -> {})).isFalse(); // closed: refused
    assertThat(lane.available()).isEqualTo(1);
  }
}
