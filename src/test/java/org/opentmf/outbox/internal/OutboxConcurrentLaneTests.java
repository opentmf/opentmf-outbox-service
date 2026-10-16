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
 * rest; queued sends run in claim order; the room the relay may claim - as deep as the sends allow
 * (1.9.0) - and the nudge half-way down;
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
  void beforeASendIsMeasured_theRoomIsTheFreeSlots_nothingAhead() {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(4, 100, false, Duration.ofSeconds(5));

    assertThat(lane.depth()).isZero();
    assertThat(lane.room()).isEqualTo(4); // the free slots, not the capacity
    lane.close();
  }

  /**
   * OUTBOX-QUEUED-LEASE-STARVATION-1 (1.9.0): the depth follows the sends - fast sends claim ahead
   * up to the capacity, as since 1.6.0; sends slower than the horizon per slot claim nothing ahead:
   * only the free slots, so no row waits behind a busy one while its lease runs.
   */
  @Test
  void theDepthFollowsTheSends_fastClaimAheadToTheCapacity_slowOnlyTheFreeSlots() {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(8, 100, false, Duration.ofSeconds(5));

    lane.sendTook(Duration.ofMillis(5).toNanos()); // 8 slots x 1 s / 5 ms = 1600: the capacity
    assertThat(lane.depth()).isEqualTo(100);
    assertThat(lane.room()).isEqualTo(100);

    OutboxConcurrentLane slow = new OutboxConcurrentLane(8, 100, false, Duration.ofSeconds(5));
    slow.sendTook(Duration.ofSeconds(36).toNanos()); // 8 x 1 s / 36 s < 1: nothing ahead
    assertThat(slow.depth()).isZero();
    assertThat(slow.room()).isEqualTo(8);

    OutboxConcurrentLane between = new OutboxConcurrentLane(8, 100, false, Duration.ofSeconds(5));
    between.sendTook(Duration.ofMillis(200).toNanos()); // 8 x 1 s / 200 ms = 40 rows ahead
    assertThat(between.depth()).isEqualTo(40);
    assertThat(between.room()).isEqualTo(48); // the free slots plus the depth
    lane.close();
    slow.close();
    between.close();
  }

  /**
   * 1.9.0 (K1, the flow shape): a slot busy with a send older than the horizon starts nothing
   * soon, whatever the average says - a lane whose average is fast queues nothing behind it.
   */
  @Test
  void aSlotBusyWithASendOlderThanTheHorizon_isNotLive_andTheDepthLeavesItOut() {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(2, 100, false, Duration.ofSeconds(5));
    lane.sendTook(Duration.ofMillis(1).toNanos()); // fast on average: 2 x 1 s / 1 ms - capacity
    assertThat(lane.liveSlots()).isEqualTo(2);
    assertThat(lane.depth()).isEqualTo(100);
    CountDownLatch release = new CountDownLatch(1);
    lane.submit(
        () -> {
          try {
            release.await(10, TimeUnit.SECONDS); // a slow send in one slot
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        });
    await()
        .pollDelay(OutboxConcurrentLane.HORIZON.plusMillis(100))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> assertThat(lane.liveSlots()).isEqualTo(1));
    assertThat(lane.depth()).isEqualTo(100); // one live slot still covers the capacity

    lane.sendTook(Duration.ofMillis(500).toNanos()); // the average now 63 ms: 1 x 1 s / 63 ms
    assertThat(lane.depth()).isEqualTo(15);
    release.countDown();
    await().atMost(Duration.ofSeconds(5)).until(() -> lane.liveSlots() == 2);
    lane.close();
  }

  @Test
  void everySlotBusyWithAnOldSend_queuesNothingAhead() {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(2, 100, false, Duration.ofSeconds(5));
    lane.sendTook(Duration.ofMillis(1).toNanos());
    CountDownLatch release = new CountDownLatch(1);
    for (int i = 0; i < 2; i++) {
      lane.submit(
          () -> {
            try {
              release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            }
          });
    }
    await()
        .pollDelay(OutboxConcurrentLane.HORIZON.plusMillis(100))
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> assertThat(lane.liveSlots()).isZero());
    assertThat(lane.depth()).isZero();
    assertThat(lane.room()).isZero(); // no free slot, nothing ahead: the rows stay for other pods
    release.countDown();
    lane.close();
  }

  /**
   * 1.9.0 (K1): rows queued in the second before the slots turned slow are given back once no slot
   * is live - unsent - and only then; a send without a row is never given back.
   */
  @Test
  void theQueue_isGivenBack_onlyWhileNoSlotIsLive() {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(1, 100, false, Duration.ofSeconds(5));
    lane.sendTook(Duration.ofMillis(1).toNanos());
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger sent = new AtomicInteger();
    lane.submit(
        1L,
        () -> {
          try {
            release.await(10, TimeUnit.SECONDS); // the one slot, a slow send
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        });
    await().atMost(Duration.ofSeconds(5)).until(() -> lane.available() == 0);
    lane.submit(2L, sent::incrementAndGet);
    lane.submit(sent::incrementAndGet); // no row: never given back
    assertThat(lane.evictStranded()).isEmpty(); // the slot is live: its send is young

    await()
        .pollDelay(OutboxConcurrentLane.HORIZON.plusMillis(100))
        .atMost(Duration.ofSeconds(5))
        .until(() -> lane.liveSlots() == 0);
    assertThat(lane.evictStranded()).containsExactly(2L);
    assertThat(lane.queued()).isEqualTo(1);
    assertThat(lane.evictStranded()).isEmpty(); // nothing left to give back

    release.countDown();
    await().atMost(Duration.ofSeconds(5)).until(() -> sent.get() == 1); // only the row-less one
    lane.close();
  }

  @Test
  void theLane_measuresItsOwnSends() {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(2, 100, false, Duration.ofSeconds(5));
    AtomicInteger done = new AtomicInteger();
    assertThat(lane.depth()).isZero(); // nothing measured yet

    IntStream.range(0, 5).forEach(i -> lane.submit(done::incrementAndGet));

    await().atMost(Duration.ofSeconds(5)).until(() -> done.get() == 5);
    await() // fast sends: claim ahead up to the capacity
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> assertThat(lane.depth()).isEqualTo(100));
    lane.close();
  }

  @Test
  void theDepthIsAMovingAverage_oneSlowSendMovesItAnEighth() {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(8, 1_000_000, false, Duration.ofSeconds(5));
    lane.sendTook(Duration.ofMillis(10).toNanos()); // the first sample sets it: 800
    assertThat(lane.depth()).isEqualTo(800);

    lane.sendTook(Duration.ofMillis(90).toNanos()); // 10 + (90 - 10) / 8 = 20 ms: 400

    assertThat(lane.depth()).isEqualTo(400);
    lane.close();
  }

  @Test
  void theRoom_isNoneAboveHalfTheDepth_andHalfWayDownNudgesOnce() {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(1, 10, false, Duration.ofSeconds(5));
    lane.sendTook(Duration.ofMillis(1).toNanos()); // fast: the depth is the capacity, 10
    AtomicInteger nudges = new AtomicInteger();
    lane.onRoom(nudges::incrementAndGet);
    CountDownLatch gate = new CountDownLatch(1);
    CountDownLatch started = new CountDownLatch(1);
    assertThat(lane.room()).isEqualTo(10); // min(capacity, 1 free + 10) - 0

    lane.submit(
        () -> {
          started.countDown();
          hold(gate);
        });
    hold(started); // the one slot is busy, nothing queued
    IntStream.range(0, 5).forEach(i -> lane.submit(() -> {}));
    assertThat(lane.room()).isEqualTo(5); // half the depth queued: claim back up to it
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
