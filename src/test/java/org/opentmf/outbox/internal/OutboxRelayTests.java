package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.internal.OutboxRelayWorker.Batch;

/**
 * One relay thread; pokes drain and coalesce - however many arrive during a pass, ONE follow-up
 * pass; room in the CONCURRENT lane nudges; the ORDERED claim runs only when ORDERED rows may be
 * due; a pass survives worker exceptions; lifecycle is safe and closes the lane.
 */
class OutboxRelayTests {

  private static final Batch EMPTY = new Batch(0, 0, false, false);

  private static OutboxConcurrentLane lane() {
    return new OutboxConcurrentLane(1, 100, false, Duration.ofSeconds(1));
  }

  @Test
  void pokeBeforeStart_isIgnored() {
    OutboxConcurrentLane stoppedLane = lane();
    OutboxRelay relay =
        new OutboxRelay(mock(OutboxRelayWorker.class), new OutboxProperties(), stoppedLane);
    assertThatCode(
            () -> {
              relay.poke(); // no executor yet
              relay.pokeOrdered();
              relay.stop(); // stop before start
            })
        .doesNotThrowAnyException();
    assertThat(stoppedLane.submit(() -> {})).isFalse(); // the lane is closed all the same
  }

  @Test
  void aPokedPass_drainsWhileBatchesComeBackFull() {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    OutboxProperties properties = new OutboxProperties();
    AtomicInteger calls = new AtomicInteger();
    when(worker.relayBatch(anyBoolean()))
        .thenAnswer(
            inv ->
                calls.incrementAndGet() == 1
                    ? new Batch(properties.getBatchSize(), 0, true, false)
                    : EMPTY);
    OutboxRelay relay = new OutboxRelay(worker, properties, lane());
    relay.start();
    try {
      relay.poke();
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
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
  void aFullConcurrentClaim_drainsAgain_onlyWhileTheLaneHasRoom() throws Exception {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    OutboxConcurrentLane lane = new OutboxConcurrentLane(1, 4, false, Duration.ofSeconds(1));
    CountDownLatch gate = new CountDownLatch(1);
    CountDownLatch busy = new CountDownLatch(1);
    lane.submit(
        () -> {
          busy.countDown();
          try {
            assertThat(gate.await(10, TimeUnit.SECONDS)).isTrue();
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        });
    assertThat(busy.await(5, TimeUnit.SECONDS)).isTrue();
    AtomicInteger calls = new AtomicInteger();
    when(worker.relayBatch(anyBoolean()))
        .thenAnswer(
            inv -> {
              if (calls.incrementAndGet() == 1) {
                lane.submit(() -> {}); // the lane fills up: no room after this claim
                lane.submit(() -> {});
                lane.submit(() -> {});
                return new Batch(0, 3, false, true);
              }
              return new Batch(0, 1, false, true); // full, and room again
            });
    OutboxRelay relay = new OutboxRelay(worker, new OutboxProperties(), lane);

    relay.relayPass(); // full - but the queue is above half: no second claim

    assertThat(calls.get()).isEqualTo(1);
    gate.countDown();
    lane.close();
  }

  @Test
  void aFailingPass_neverKillsTheRelay() {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    AtomicInteger calls = new AtomicInteger();
    when(worker.relayBatch(anyBoolean()))
        .thenAnswer(
            inv -> {
              if (calls.incrementAndGet() == 1) {
                throw new RuntimeException("transient");
              }
              return EMPTY;
            });
    OutboxRelay relay = new OutboxRelay(worker, new OutboxProperties(), lane());
    relay.start();
    try {
      relay.poke(); // throws inside - swallowed
      await().atMost(Duration.ofSeconds(5)).until(() -> calls.get() >= 1);
      relay.poke(); // still serviced (after the first pass ran: back-to-back pokes coalesce)
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> assertThat(calls.get()).isGreaterThanOrEqualTo(2));
    } finally {
      relay.stop();
    }
    relay.poke(); // after stop - ignored, never throws
  }

  @Test
  void roomInTheLane_nudgesTheRelay_andStopClosesTheLane() {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    AtomicInteger calls = new AtomicInteger();
    when(worker.relayBatch(anyBoolean()))
        .thenAnswer(
            inv -> {
              calls.incrementAndGet();
              return EMPTY;
            });
    OutboxConcurrentLane lane = lane();
    OutboxRelay relay = new OutboxRelay(worker, new OutboxProperties(), lane);
    relay.start();
    try {
      assertThat(lane.submit(() -> {})).isTrue(); // the worker runs dry - the relay is nudged
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> assertThat(calls.get()).isGreaterThanOrEqualTo(1));
    } finally {
      relay.stop();
    }
    assertThat(lane.submit(() -> {})).isFalse(); // closed with the relay
  }

  @Test
  void nudgesWhileAPassIsQueued_coalesceIntoIt() throws Exception {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    CountDownLatch inFirstPass = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    when(worker.relayBatch(anyBoolean()))
        .thenAnswer(
            inv -> {
              if (calls.incrementAndGet() == 1) {
                inFirstPass.countDown();
                release.await();
              }
              return EMPTY;
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

  /**
   * The claim-cadence pin (OUTBOX-RELAY-CLAIM-CADENCE-1): fifty sends finish during one pass -
   * every one returns its slot, the queue crosses half-way, the workers run dry - and the relay
   * makes ONE follow-up pass, never one per released slot.
   */
  @Test
  void fiftyReleasesDuringAPass_makeOneFollowUpPass() throws Exception {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    OutboxConcurrentLane lane = new OutboxConcurrentLane(8, 100, false, Duration.ofSeconds(1));
    CountDownLatch inFirstPass = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger calls = new AtomicInteger();
    when(worker.relayBatch(anyBoolean()))
        .thenAnswer(
            inv -> {
              if (calls.incrementAndGet() == 1) {
                inFirstPass.countDown();
                release.await();
              }
              return EMPTY;
            });
    OutboxRelay relay = new OutboxRelay(worker, new OutboxProperties(), lane);
    relay.start();
    try {
      relay.poke();
      assertThat(inFirstPass.await(5, TimeUnit.SECONDS)).isTrue();
      AtomicInteger sent = new AtomicInteger();
      for (int i = 0; i < 50; i++) {
        lane.submit(sent::incrementAndGet);
      }
      await().atMost(Duration.ofSeconds(5)).until(() -> sent.get() == 50);
      await().atMost(Duration.ofSeconds(5)).until(() -> lane.available() == 8);
      release.countDown();
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> assertThat(calls.get()).isEqualTo(2));
      await()
          .during(Duration.ofMillis(300))
          .atMost(Duration.ofSeconds(1))
          .until(() -> calls.get() == 2);
    } finally {
      relay.stop();
    }
  }

  @Test
  void theOrderedClaim_runsOnlyWhenOrderedRowsMayBeDue() {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    List<Boolean> withOrdered = new CopyOnWriteArrayList<>();
    AtomicInteger fullOrdered = new AtomicInteger();
    when(worker.relayBatch(anyBoolean()))
        .thenAnswer(
            inv -> {
              withOrdered.add(inv.getArgument(0));
              return fullOrdered.getAndDecrement() > 0 ? new Batch(100, 0, true, false) : EMPTY;
            });
    OutboxRelay relay = new OutboxRelay(worker, new OutboxProperties(), lane());

    relay.relayPass(); // the first pass: whatever was pending at start
    relay.relayPass(); // a CONCURRENT nudge: one claim, not two
    relay.pokeOrdered(); // not started: only arms the claim
    relay.relayPass();
    relay.relayPass();
    relay.sweep(); // the sweep always claims it
    fullOrdered.set(1);
    relay.pokeOrdered();
    relay.relayPass(); // a full ORDERED batch: claimed again at once

    assertThat(withOrdered).containsExactly(true, false, true, false, true, true, true);
  }

  @Test
  void anOrderedNudge_runsAPassThatClaimsTheOrderedLane() {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    List<Boolean> withOrdered = new CopyOnWriteArrayList<>();
    when(worker.relayBatch(anyBoolean()))
        .thenAnswer(
            inv -> {
              withOrdered.add(inv.getArgument(0));
              return EMPTY;
            });
    OutboxProperties properties = new OutboxProperties();
    properties.setSweepInterval(Duration.ofHours(1)); // only the nudges run passes
    OutboxRelay relay = new OutboxRelay(worker, properties, lane());
    relay.start();
    try {
      relay.poke(); // the first pass claims the ORDERED lane anyway
      await().atMost(Duration.ofSeconds(5)).until(() -> withOrdered.size() == 1);
      relay.pokeOrdered();
      await().atMost(Duration.ofSeconds(5)).until(() -> withOrdered.size() == 2);
      relay.poke();
      await().atMost(Duration.ofSeconds(5)).until(() -> withOrdered.size() == 3);

      assertThat(withOrdered).containsExactly(true, true, false);
    } finally {
      relay.stop();
    }
  }

  @Test
  void aFailedPass_claimsTheOrderedLaneAgainNextTime() {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    List<Boolean> withOrdered = new CopyOnWriteArrayList<>();
    AtomicInteger calls = new AtomicInteger();
    when(worker.relayBatch(anyBoolean()))
        .thenAnswer(
            inv -> {
              withOrdered.add(inv.getArgument(0));
              if (calls.incrementAndGet() == 2) {
                throw new IllegalStateException("database gone mid-claim");
              }
              return EMPTY;
            });
    OutboxRelay relay = new OutboxRelay(worker, new OutboxProperties(), lane());

    relay.relayPass();
    relay.relayPass(); // fails
    relay.relayPass();

    assertThat(withOrdered).containsExactly(true, false, true);
  }
}
