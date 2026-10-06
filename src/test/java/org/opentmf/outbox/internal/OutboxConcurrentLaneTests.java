package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Permits bound the sends; threads are daemons; close waits up to the grace for sends. */
class OutboxConcurrentLaneTests {

  @Test
  void thePermits_areTheBound() {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(1, false, Duration.ofSeconds(1));
    AtomicInteger nudges = new AtomicInteger();
    lane.onRelease(nudges::incrementAndGet);

    assertThat(lane.tryAcquire()).isTrue();
    assertThat(lane.tryAcquire()).isFalse();
    assertThat(lane.available()).isZero();
    lane.release();
    assertThat(lane.available()).isEqualTo(1);
    assertThat(nudges.get()).isEqualTo(1); // a released permit nudges the relay
    lane.close();
  }

  @Test
  void aSend_runsOnADaemonLaneThread() throws Exception {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(1, false, Duration.ofSeconds(1));
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
  void close_letsASendInFlightFinishWithinTheGrace() throws Exception {
    OutboxConcurrentLane lane = new OutboxConcurrentLane(1, false, Duration.ofSeconds(5));
    CountDownLatch started = new CountDownLatch(1);
    AtomicBoolean finished = new AtomicBoolean();
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

    lane.close(); // waits up to the grace

    assertThat(finished.get()).isTrue();
    assertThat(lane.submit(() -> {})).isFalse(); // closed: refused, permit returned
  }
}
