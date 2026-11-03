package org.opentmf.outbox.internal;

import java.time.Duration;
import java.util.function.LongSupplier;
import lombok.extern.slf4j.Slf4j;

/**
 * One transport's claim pause on one pod (OUTBOX-FAILING-RELAY-CLAIMS-1, 1.11.0): after {@code
 * after-failures} sends over the transport in a row have FAILED with the transport out of reach
 * ({@link org.opentmf.outbox.OutboxPublisher#transportDown}) and no success between them, the
 * pod takes none of the transport's rows but one PROBE now and then - the first a {@code
 * backoff-base} after the pause, each next one twice as long after the one before, up to {@code
 * claim-pause.max}. The rest it leaves to the other pods. One successful send over the transport
 * ends the pause.
 *
 * <p>Why: a pod whose transport is down (its broker unreachable from that pod, an expired client
 * credential, a broken egress) fails every row it claims, and every such row pays an attempt and
 * waits out its backoff - while another pod could have sent it at once. With instant failures it
 * is even the FASTEST claimer, its slots freeing at once; about half an hour of it parks rows a
 * healthy pod would have sent. Scoped to the transport - never to a lane or a receiver: a
 * receiver that is down fails on every pod, and pausing its lane would hold back every other
 * receiver's rows on it (measured: one dead HTTP subscriber beside a healthy one let 354 of
 * 1,000 healthy rows through in a minute under a lane-wide pause, all 1,000 without).
 *
 * <p>Only a SEND's verdict counts: a booking that failed, a lapsed lease counted as an attempt and
 * a {@link org.opentmf.outbox.TerminalOutboxException} (the receiver answered - the transport
 * works) neither pause nor resume. Thread-safe: the senders record, the relay thread asks.
 */
@Slf4j
final class OutboxClaimPause {

  private final String transport;
  private final int afterFailures;
  private final Duration first;
  private final Duration max;
  private final LongSupplier nanos;
  private final OutboxMetrics metrics;

  private int failuresInARow;
  private boolean paused;
  private long nextProbeAt;
  private Duration interval;

  OutboxClaimPause(
      String transport,
      int afterFailures,
      Duration first,
      Duration max,
      LongSupplier nanos,
      OutboxMetrics metrics) {
    this.transport = transport;
    this.afterFailures = afterFailures;
    this.first = first.compareTo(max) <= 0 ? first : max;
    this.max = max;
    this.nanos = nanos;
    this.metrics = metrics;
  }

  /**
   * Whether this pod may take one of the transport's rows now: always unless paused; while paused
   * only as the probe once its time has come - the next probe's time then moves on.
   */
  synchronized boolean mayTake() {
    if (!paused) {
      return true;
    }
    long now = nanos.getAsLong();
    if (now - nextProbeAt < 0) {
      return false;
    }
    Duration doubled = interval.multipliedBy(2);
    interval = doubled.compareTo(max) <= 0 ? doubled : max;
    nextProbeAt = now + interval.toNanos();
    return true;
  }

  /** A send succeeded: the transport works - a pause ends, the failure run starts over. */
  synchronized void succeeded() {
    failuresInARow = 0;
    if (paused) {
      paused = false;
      metrics.claimsPaused(transport, false);
      log.warn("Outbox claims of {} rows resumed on this pod - a send succeeded", transport);
    }
  }

  /** A send failed, the transport out of reach: at {@code after-failures} in a row, a pause. */
  synchronized void failed(RuntimeException cause) {
    failuresInARow++;
    if (paused || afterFailures == 0 || failuresInARow < afterFailures) {
      return;
    }
    paused = true;
    interval = first;
    nextProbeAt = nanos.getAsLong() + first.toNanos();
    metrics.claimsPaused(transport, true);
    log.warn(
        "Outbox claims of {} rows PAUSED on this pod after {} failed sends in a row (last: {}) -"
            + " one probe row after {}, then at doubling intervals up to {}, until a send"
            + " succeeds; the other pods take the rest",
        transport,
        failuresInARow,
        cause.toString(),
        first,
        max);
  }

  synchronized boolean paused() {
    return paused;
  }
}
