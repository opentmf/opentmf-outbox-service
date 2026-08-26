package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/** The S23 backoff arithmetic: exponential from base, capped, attempts >= 1. */
class OutboxBackoffTests {

  private final OutboxBackoff backoff = new OutboxBackoff(new OutboxProperties());

  @Test
  void firstFailure_waitsTheBase() {
    assertThat(backoff.delayFor(1)).isEqualTo(Duration.ofSeconds(5));
  }

  @Test
  void growthIsExponential_untilTheCap() {
    assertThat(backoff.delayFor(2)).isEqualTo(Duration.ofSeconds(10));
    assertThat(backoff.delayFor(4)).isEqualTo(Duration.ofSeconds(40));
    assertThat(backoff.delayFor(30)).isEqualTo(Duration.ofMinutes(10)); // capped
  }

  @Test
  void zeroAttempts_isACallerBug() {
    assertThatIllegalArgumentException().isThrownBy(() -> backoff.delayFor(0));
  }
}
