package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxProperties;

/**
 * One relay thread; pokes drain and coalesce; a freed CONCURRENT slot nudges; a pass survives
 * worker exceptions; lifecycle is safe and closes the lane.
 */
class OutboxRelayTests {

  private static OutboxConcurrentLane lane() {
    return new OutboxConcurrentLane(1, false, Duration.ofSeconds(1));
  }

  @Test
  void pokeBeforeStart_isIgnored() {
    OutboxRelay relay =
        new OutboxRelay(mock(OutboxRelayWorker.class), new OutboxProperties(), lane());
    assertThatCode(
            () -> {
              relay.poke(); // no executor yet
              relay.stop(); // stop before start
            })
        .doesNotThrowAnyException();
  }

  @Test
  void aPokedPass_drainsWhileBatchesComeBackFull() {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    OutboxProperties properties = new OutboxProperties();
    AtomicInteger calls = new AtomicInteger();
    when(worker.relayBatch())
        .thenAnswer(inv -> calls.incrementAndGet() == 1 ? properties.getBatchSize() : 0);
    OutboxRelay relay = new OutboxRelay(worker, properties, lane());
    relay.start();
    try {
      relay.poke();
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(calls.get()).isGreaterThanOrEqualTo(2)); // full batch drained again
      // the relay thread is a DAEMON - it must never hold the JVM open on shutdown
      assertThat(
              Thread.getAllStackTraces().keySet().stream()
                  .filter(t -> "opentmf-outbox-relay".equals(t.getName()))
                  .findFirst())
          .hasValueSatisfying(t -> assertThat(t.isDaemon()).isTrue());
    } finally {
      relay.stop();
    }
  }

  @Test
  void aFailingPass_neverKillsTheRelay() {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    AtomicInteger calls = new AtomicInteger();
    when(worker.relayBatch())
        .thenAnswer(
            inv -> {
              if (calls.incrementAndGet() == 1) {
                throw new RuntimeException("transient");
              }
              return 0;
            });
    OutboxRelay relay = new OutboxRelay(worker, new OutboxProperties(), lane());
    relay.start();
    try {
      relay.poke(); // throws inside - swallowed
      await().atMost(Duration.ofSeconds(5)).until(() -> calls.get() >= 1);
      relay.poke(); // still serviced (after the first pass ran: back-to-back pokes coalesce)
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(calls.get()).isGreaterThanOrEqualTo(2));
    } finally {
      relay.stop();
    }
    relay.poke(); // after stop - ignored, never throws
  }

  @Test
  void aFreedSlot_nudgesTheRelay_andStopClosesTheLane() {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    AtomicInteger calls = new AtomicInteger();
    when(worker.relayBatch()).thenAnswer(inv -> calls.incrementAndGet() * 0);
    OutboxConcurrentLane lane = lane();
    OutboxRelay relay = new OutboxRelay(worker, new OutboxProperties(), lane);
    relay.start();
    try {
      assertThat(lane.tryAcquire()).isTrue();
      lane.release(); // the slot returns - the relay is nudged to fill it
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(calls.get()).isGreaterThanOrEqualTo(1));
    } finally {
      relay.stop();
    }
    assertThat(lane.tryAcquire()).isTrue();
    assertThat(lane.submit(() -> {})).isFalse(); // closed with the relay
  }

  @Test
  void nudgesWhileAPassIsQueued_coalesceIntoIt() throws Exception {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    CountDownLatch inFirstPass = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    when(worker.relayBatch())
        .thenAnswer(
            inv -> {
              if (calls.incrementAndGet() == 1) {
                inFirstPass.countDown();
                release.await();
              }
              return 0;
            });
    OutboxRelay relay = new OutboxRelay(worker, new OutboxProperties(), lane());
    relay.start();
    try {
      relay.poke();
      assertThat(inFirstPass.await(5, TimeUnit.SECONDS)).isTrue();
      for (int i = 0; i < 10; i++) {
        relay.poke(); // the first queues ONE pass, the other nine ride it
      }
      release.countDown();
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> assertThat(calls.get()).isEqualTo(2));
      await() // and no third pass follows
          .during(Duration.ofMillis(200))
          .atMost(Duration.ofSeconds(1))
          .until(() -> calls.get() == 2);
    } finally {
      relay.stop();
    }
  }
}
