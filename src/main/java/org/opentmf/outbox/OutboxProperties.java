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

  /**
   * Maximum rows one claim takes per lane - and the CONCURRENT lane's hand-off queue at most: the
   * relay claims CONCURRENT rows ahead of the slots (1.6.0), as deep as the slots start within
   * about a second and never deeper than this (1.9.0 - none behind slow sends). Default 100.
   */
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

  /**
   * Upper bound the relay waits for a publish acknowledgement - and, since 1.9.0, how long a send
   * may run at least before it is presumed hung: the send heartbeat keeps a running send's lease
   * up to the larger of its lease and this, never further. Default 10s.
   */
  @NotNull private Duration sendTimeout = Duration.ofSeconds(10);

  /**
   * The library default LEASE of a CONCURRENT row (a publisher may declare its own): how long a
   * claimed row stays its holder's without a word from it. While a send runs, its pod renews the
   * lease every third of it (1.9.0), up to the larger of the lease and {@code send-timeout}; a pod
   * that dies stops renewing, and its rows are taken over once their leases lapse. Default 2min.
   */
  @NotNull private Duration lease = Duration.ofMinutes(2);

  /**
   * How long a stopping relay lets in-flight sends finish (and book) before it gives up on
   * them; a send still running then is booked by nobody and its row is redelivered once its
   * lease lapses. Default 10s.
   */
  @NotNull private Duration shutdownGrace = Duration.ofSeconds(10);

  /**
   * How often the gauges (pending, parked, in-flight, relay-lag) are re-read from the database. A
   * scrape reads the last values and never waits on the database; relay-lag is computed at read
   * time from the oldest open row's instant, so it keeps growing between refreshes. Default 15s.
   */
  @NotNull private Duration metricsRefresh = Duration.ofSeconds(15);

  /** The bounded bulk operations: prune, unpark by filter ({@code opentmf.outbox.maintenance}). */
  @Valid private final Maintenance maintenance = new Maintenance();

  /** The ORDERED lane ({@code opentmf.outbox.ordered.*}). */
  @Valid private final Ordered ordered = new Ordered();

  /** The CONCURRENT lane ({@code opentmf.outbox.concurrent.*}). */
  @Valid private final Concurrent concurrent = new Concurrent();

  /** The library's default Kafka publisher ({@code opentmf.outbox.kafka.*}, 1.5.0). */
  @Valid private final Kafka kafka = new Kafka();

  /** The library's HTTP publisher's plain client ({@code opentmf.outbox.http.*}, 1.9.0). */
  @Valid private final Http http = new Http();

  /**
   * The longest a CONCURRENT send of the library's default lease runs before it is presumed hung:
   * the larger of {@code lease} and {@code send-timeout} - the send heartbeat renews no further.
   */
  public Duration sendBound() {
    return lease.compareTo(sendTimeout) >= 0 ? lease : sendTimeout;
  }

  /**
   * The plain HTTP client's read timeout: {@code http.read-timeout} when set, else the send bound
   * less a tenth of it (at least one second) - so an HTTP call that hangs fails as an attempt of
   * its own before its lease is left to lapse.
   */
  public Duration httpReadTimeout() {
    if (http.readTimeout != null) {
      return http.readTimeout;
    }
    Duration bound = sendBound();
    Duration margin = bound.dividedBy(10);
    return bound.minus(margin.compareTo(Duration.ofSeconds(1)) >= 0 ? margin : Duration.ofSeconds(1));
  }

  /** The plain HTTP client's connect timeout must be positive. */
  @AssertTrue(message = "opentmf.outbox.http.connect-timeout must be positive")
  public boolean isHttpConnectTimeoutPositive() {
    return http.connectTimeout == null || positive(http.connectTimeout);
  }

  /** A set HTTP read timeout must end before the send bound - or a hung call outlives its lease. */
  @AssertTrue(
      message =
          "opentmf.outbox.http.read-timeout must be positive and below the larger of"
              + " opentmf.outbox.lease and opentmf.outbox.send-timeout")
  public boolean isHttpReadTimeoutWithinTheSendBound() {
    if (http.readTimeout == null || lease == null || sendTimeout == null) {
      return true;
    }
    return positive(http.readTimeout) && http.readTimeout.compareTo(sendBound()) < 0;
  }
  /**
   * The ORDERED lease must outlast one Kafka send, or a row whose acknowledgement is slow is
   * re-claimed while it is still being sent.
   */
  @AssertTrue(message = "opentmf.outbox.ordered.lease must exceed opentmf.outbox.send-timeout")
  public boolean isOrderedLeaseLongerThanSendTimeout() {
    return ordered.lease == null || sendTimeout == null || ordered.lease.compareTo(sendTimeout) > 0;
  }

  /**
   * A Kafka row on the CONCURRENT lane holds its claim for {@code kafka.lease}: it must outlast
   * one Kafka send, as the ORDERED lease must.
   */
  @AssertTrue(message = "opentmf.outbox.kafka.lease must exceed opentmf.outbox.send-timeout")
  public boolean isKafkaLeaseLongerThanSendTimeout() {
    return kafka.lease == null || sendTimeout == null || kafka.lease.compareTo(sendTimeout) > 0;
  }

  private static boolean positive(Duration duration) {
    return !duration.isNegative() && !duration.isZero(); // Duration.isPositive is Java 18+
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

  /** The bounded bulk operations' settings (the prune, the unpark by filter). */
  @Getter
  @Setter
  public static class Maintenance {

    /** Rows per batch - each batch is its own short transaction. Default 5000. */
    @Positive private int batchSize = 5_000;

    /**
     * How long one call keeps working through batches before it answers; a call that stops on
     * the budget says more remains, and the caller calls again. Default 10s.
     */
    @NotNull private Duration timeBudget = Duration.ofSeconds(10);
  }

  /**
   * The default Kafka publisher's settings (1.5.0): which lane its rows ride and, on the
   * CONCURRENT lane, what orders them. Stamped on each row at APPEND - a change applies to the
   * rows written after it; rows already pending keep the lane they were stamped with.
   */
  @Getter
  @Setter
  public static class Kafka {

    /**
     * {@code ORDERED} (the default, as before 1.5.0): the one ORDERED thread of each pod, in
     * {@code id} order - two pods never take one row twice, but across pods rows interleave, so
     * strict order is NOT promised with several relays. {@code CONCURRENT}: rows of one ordering
     * key are never in flight together, within a pod or across pods, and go in {@code id} order on
     * the happy path; different keys relay in parallel.
     */
    @NotNull private OutboxPublisher.Lane lane = OutboxPublisher.Lane.ORDERED;

    /**
     * On the CONCURRENT lane, what orders a Kafka row: {@code AGGREGATE_ID} (the default) - one
     * aggregate's rows in order, whichever pod relays them, the same key the record already
     * carries; {@code NONE} - no order at all, every row independent. Ignored on ORDERED.
     */
    @NotNull private KafkaOrderingKey orderingKey = KafkaOrderingKey.AGGREGATE_ID;

    /**
     * The lease of a Kafka row on the CONCURRENT lane - short, as the ORDERED lease is: after a
     * pod stop an unbooked row waits this long before another relay takes it. Must exceed
     * {@code send-timeout}. Ignored on ORDERED (the {@code ordered.lease} governs). Default 15s.
     */
    @NotNull private Duration lease = Duration.ofSeconds(15);
  }

  /**
   * The library's HTTP publisher's PLAIN client (1.9.0) - the client of a row that names no
   * client profile and matches no profile's base-url. Up to 1.8.0 it had no timeout at all: a
   * receiver that never answered held its send, and its lease, for as long as it liked. A
   * profile's client (from the consumer's {@code OutboxClientProfileResolver}) keeps its own
   * timeouts and must set them below its rows' lease.
   */
  @Getter
  @Setter
  public static class Http {

    /** Connect timeout of the plain client; positive. Default 10s. */
    @NotNull private Duration connectTimeout = Duration.ofSeconds(10);

    /**
     * Read timeout of the plain client; unset (the default) = the larger of {@code lease} and
     * {@code send-timeout}, less a tenth (at least one second). When set, it must be below that
     * larger value.
     */
    private Duration readTimeout;
  }

  /** What orders a Kafka row on the CONCURRENT lane ({@code opentmf.outbox.kafka.ordering-key}). */
  public enum KafkaOrderingKey {
    /** The row's {@code aggregateId} - one aggregate's rows in order. */
    AGGREGATE_ID,
    /** None - every row independent, no order promised. */
    NONE
  }

  /** The CONCURRENT lane's settings. */
  @Getter
  @Setter
  public static class Concurrent {

    /**
     * Sends in flight at once, per relay - and on platform threads also the thread bound. The
     * relay claims ahead of the slots into a hand-off queue (1.6.0) as deep as they start within
     * about a second, at most {@code batch-size} rows (1.9.0); a row that waited there a tenth of
     * its lease or longer is leased again before its send. Default 8.
     */
    @Positive private int maxInFlight = 8;
  }
}
