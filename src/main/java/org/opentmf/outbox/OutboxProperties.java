package org.opentmf.outbox;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Typed configuration for the transactional outbox, bound under the LIBRARY prefix
 * ({@code opentmf.outbox.*} - env {@code OPENTMF_OUTBOX_*}, one underscore per word: a
 * collapsed compound form does not bind on Boot 4).
 */
@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "opentmf.outbox")
public class OutboxProperties {

  /** Fixed delay between relay sweep passes - the safety net behind the nudge. Default 5s. */
  @NotNull private Duration sweepInterval = Duration.ofSeconds(5);

  /** Maximum rows claimed per relay batch. */
  @Positive private int batchSize = 100;

  /**
   * Failed-delivery attempts after which a row is EXHAUSTED (parked by default) - the LIBRARY
   * budget, used when the row's publisher declares none. Default 10.
   */
  @Positive private int maxAttempts = 10;

  /** Exponential-backoff base delay after the first failed attempt. Default 5s. */
  @NotNull private Duration backoffBase = Duration.ofSeconds(5);

  /** Exponential-backoff multiplier per further failed attempt (a double). Default 2.0. */
  @DecimalMin("1.0")
  private double backoffFactor = 2.0;

  /** Exponential-backoff ceiling. Default 10min. */
  @NotNull private Duration backoffCap = Duration.ofMinutes(10);

  /**
   * Relayed and cancelled rows older than this are pruned; parked rows NEVER auto-prune.
   * Default 7 days.
   */
  @NotNull private Duration retention = Duration.ofDays(7);

  /** Upper bound the relay waits for a publish acknowledgement. */
  @NotNull private Duration sendTimeout = Duration.ofSeconds(10);

  /**
   * The library default LEASE of a CONCURRENT row (a publisher may declare its own): the claim
   * lasts this long, so it must exceed the longest call of the lane's publishers. Default 2min.
   */
  @NotNull private Duration lease = Duration.ofMinutes(2);

  /**
   * How long a stopping relay lets in-flight sends finish (and book) before it gives up on
   * them; a send still running then is booked by nobody and its row is redelivered once its
   * lease lapses. Default 10s.
   */
  @NotNull private Duration shutdownGrace = Duration.ofSeconds(10);

  /** The ORDERED lane ({@code opentmf.outbox.ordered.*}). */
  @Valid private final Ordered ordered = new Ordered();

  /** The CONCURRENT lane ({@code opentmf.outbox.concurrent.*}). */
  @Valid private final Concurrent concurrent = new Concurrent();

  /**
   * The ORDERED lease must outlast one Kafka send, or a row whose acknowledgement is slow is
   * re-claimed while it is still being sent.
   */
  @AssertTrue(message = "opentmf.outbox.ordered.lease must exceed opentmf.outbox.send-timeout")
  public boolean isOrderedLeaseLongerThanSendTimeout() {
    return ordered.lease == null || sendTimeout == null || ordered.lease.compareTo(sendTimeout) > 0;
  }

  /** The ORDERED lane's settings. */
  @Getter
  @Setter
  public static class Ordered {

    /**
     * The library default LEASE of an ORDERED row - SHORT on purpose: after a pod stop an
     * unbooked row waits this long before another relay takes it. Each row's lease is renewed
     * right before its send, so it covers one call, never the batch. Must exceed
     * {@code send-timeout}. Default 15s.
     */
    @NotNull private Duration lease = Duration.ofSeconds(15);
  }

  /** The CONCURRENT lane's settings. */
  @Getter
  @Setter
  public static class Concurrent {

    /**
     * Sends in flight at once, per relay. The claim takes only as many CONCURRENT rows as there
     * are free slots, so no lease burns in a queue - and on platform threads this is also the
     * thread bound. Default 8.
     */
    @Positive private int maxInFlight = 8;
  }
}
