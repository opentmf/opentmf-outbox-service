package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** One transport's claim pause on a hand-moved clock (OUTBOX-FAILING-RELAY-CLAIMS-1). */
class OutboxClaimPauseTests {

  private static final RuntimeException DOWN = new IllegalStateException("broker unreachable");

  private final AtomicLong nanos = new AtomicLong(1_000_000_000L);
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final OutboxMetrics metrics =
      new OutboxMetrics(registry, mock(OutboxEventRepository.class), Duration.ofSeconds(15));
  private final OutboxClaimPause pause =
      new OutboxClaimPause(
          "kafka", 5, Duration.ofSeconds(5), Duration.ofSeconds(30), nanos::get, metrics);

  private void after(Duration duration) {
    nanos.addAndGet(duration.toNanos());
  }

  private void fail(int times) {
    for (int i = 0; i < times; i++) {
      pause.failed(DOWN);
    }
  }

  private double gauge() {
    return registry
        .get(OutboxMetrics.CLAIMS_PAUSED)
        .tag(OutboxMetrics.TAG_TRANSPORT, "kafka")
        .gauge()
        .value();
  }

  @Test
  void belowTheThreshold_thePodTakesEveryRow_andNoGaugeSeriesExistsYet() {
    fail(4);

    assertThat(pause.paused()).isFalse();
    assertThat(pause.mayTake()).isTrue();
    assertThat(registry.find(OutboxMetrics.CLAIMS_PAUSED).gauge()).isNull();
  }

  @Test
  void aSuccessBetweenFailures_startsTheRunOver() {
    fail(4);
    pause.succeeded();
    fail(4);

    assertThat(pause.paused()).isFalse();
  }

  @Test
  void paused_takesOneProbeRowAtDoublingIntervals_upToTheCap() {
    fail(5);
    assertThat(pause.paused()).isTrue();
    assertThat(gauge()).isEqualTo(1d);
    assertThat(pause.mayTake()).isFalse();

    after(Duration.ofSeconds(5));
    assertThat(pause.mayTake()).isTrue(); // the first probe: backoff-base after the pause
    assertThat(pause.mayTake()).isFalse();
    after(Duration.ofSeconds(9));
    assertThat(pause.mayTake()).isFalse();
    after(Duration.ofSeconds(1));
    assertThat(pause.mayTake()).isTrue(); // 10 s after the first
    after(Duration.ofSeconds(20));
    assertThat(pause.mayTake()).isTrue(); // 20 s after the second
    after(Duration.ofSeconds(30));
    assertThat(pause.mayTake()).isTrue(); // 30 s after the third: 40 s, capped
    after(Duration.ofSeconds(29));
    assertThat(pause.mayTake()).isFalse();
    after(Duration.ofSeconds(1));
    assertThat(pause.mayTake()).isTrue();
  }

  @Test
  void oneSuccess_resumesTheTransport() {
    fail(7);
    pause.succeeded();

    assertThat(pause.paused()).isFalse();
    assertThat(pause.mayTake()).isTrue();
    assertThat(gauge()).isZero();
    fail(4);
    assertThat(pause.paused()).isFalse(); // the run starts over
  }


  @Test
  void afterFailuresZero_neverPauses() {
    OutboxClaimPause off =
        new OutboxClaimPause(
            "kafka", 0, Duration.ofSeconds(5), Duration.ofSeconds(30), nanos::get, metrics);
    for (int i = 0; i < 100; i++) {
      off.failed(DOWN);
    }

    assertThat(off.paused()).isFalse();
    assertThat(off.mayTake()).isTrue();
  }

  @Test
  void aBaseAboveTheCap_probesAtTheCap() {
    OutboxClaimPause capped =
        new OutboxClaimPause(
            "kafka", 1, Duration.ofMinutes(5), Duration.ofSeconds(30), nanos::get, metrics);
    capped.failed(DOWN);

    after(Duration.ofSeconds(30));
    assertThat(capped.mayTake()).isTrue();
  }
}
