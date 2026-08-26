package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** One relay thread; pokes drain; a pass survives worker exceptions; lifecycle is safe. */
class OutboxRelayTests {

  @Test
  void pokeBeforeStart_isIgnored() {
    OutboxRelay relay = new OutboxRelay(mock(OutboxRelayWorker.class), new OutboxProperties());
    relay.poke(); // no executor yet - must not throw
    relay.stop(); // stop before start - must not throw
  }

  @Test
  void aPokedPass_drainsWhileBatchesComeBackFull() {
    OutboxRelayWorker worker = mock(OutboxRelayWorker.class);
    OutboxProperties properties = new OutboxProperties();
    AtomicInteger calls = new AtomicInteger();
    when(worker.relayBatch())
        .thenAnswer(inv -> calls.incrementAndGet() == 1 ? properties.getBatchSize() : 0);
    OutboxRelay relay = new OutboxRelay(worker, properties);
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
    OutboxRelay relay = new OutboxRelay(worker, new OutboxProperties());
    relay.start();
    try {
      relay.poke(); // throws inside - swallowed
      relay.poke(); // still serviced
      await().atMost(Duration.ofSeconds(5)).untilAsserted(
          () -> assertThat(calls.get()).isGreaterThanOrEqualTo(2));
    } finally {
      relay.stop();
    }
    relay.poke(); // after stop - ignored, never throws
  }
}
