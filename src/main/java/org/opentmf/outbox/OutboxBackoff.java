package org.opentmf.outbox;

import java.time.Duration;
import lombok.RequiredArgsConstructor;

/**
 * The S23 retry-backoff arithmetic: exponential, {@code base * factor^(attempts-1)}, capped -
 * logical defaults base 5s, factor 2, cap 10min (all from {@link OutboxProperties}).
 */
@RequiredArgsConstructor
class OutboxBackoff {

  private final OutboxProperties properties;

  /**
   * Delay before the next delivery attempt.
   *
   * @param attempts the failed-attempt count AFTER the failure being scheduled (at least 1)
   * @return {@code min(cap, base * factor^(attempts-1))}
   */
  public Duration delayFor(int attempts) {
    if (attempts < 1) {
      throw new IllegalArgumentException("attempts must be >= 1, was %d".formatted(attempts));
    }
    double scaled =
        properties.getBackoffBase().toMillis()
            * Math.pow(properties.getBackoffFactor(), attempts - 1d);
    long capped = (long) Math.min(scaled, properties.getBackoffCap().toMillis());
    return Duration.ofMillis(capped);
  }
}
