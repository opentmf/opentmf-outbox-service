package org.opentmf.outbox;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Typed configuration for the S23 transactional outbox, bound under the LIBRARY prefix
 * ({@code opentmf.outbox.*} - env {@code OPENTMF_OUTBOX_*}, one underscore per word: the
 * 2026-08-25 spelling finding says the collapsed compound form does not bind on Boot 4). All
 * defaults are the S23 logical defaults.
 */
@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "opentmf.outbox")
class OutboxProperties {

  /** Fixed delay between relay sweep passes - the safety net behind the nudge. Default 5s. */
  @NotNull private Duration sweepInterval = Duration.ofSeconds(5);

  /** Maximum rows claimed per relay batch. */
  @Positive private int batchSize = 100;

  /** Failed-delivery attempts after which a row PARKS (ops unpark only). Default 10. */
  @Positive private int maxAttempts = 10;

  /** Exponential-backoff base delay after the first failed attempt. Default 5s. */
  @NotNull private Duration backoffBase = Duration.ofSeconds(5);

  /** Exponential-backoff multiplier per further failed attempt. Default 2. */
  @DecimalMin("1.0")
  private double backoffFactor = 2.0;

  /** Exponential-backoff ceiling. Default 10min. */
  @NotNull private Duration backoffCap = Duration.ofMinutes(10);

  /** Relayed rows older than this are pruned; parked rows NEVER auto-prune. Default 7 days. */
  @NotNull private Duration retention = Duration.ofDays(7);

  /** Upper bound the relay waits for a publish acknowledgement. */
  @NotNull private Duration sendTimeout = Duration.ofSeconds(10);
}
