package org.opentmf.outbox.internal;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * The outbox's time (1.9.0): the DATABASE's clock, never a pod's own. Every instant the library
 * writes or compares - a lease end and its lapse, a retry's due time, a hold's release, the
 * append's {@code created_on} - is the JVM clock plus an OFFSET measured against the database
 * ({@code select clock_timestamp()}, the JVM's instant taken half-way through the round trip): at
 * startup, then every {@link #REMEASURE}. Up to 1.8.0 each pod used its own JVM clock, so a pod
 * whose clock ran ahead took rows under another pod's live lease, and a pod whose clock ran behind
 * stamped leases that had already lapsed for every other pod.
 *
 * <p>Each measurement takes {@link #SAMPLES} round trips and keeps the FASTEST - the one with the
 * least room for an asymmetric delay - and an offset within that round trip's own uncertainty
 * (half of it, plus {@link #UNCERTAINTY_FLOOR}) is taken as none: a pod that shares the
 * database's clock stays on its own, never "corrected" by a slow round trip (through a proxy, on
 * a loaded host, a single trip can be a hundred milliseconds one way). A failed measurement keeps
 * the last offset; a
 * pod that never reached the database runs on its own clock until it does. A skew above {@link
 * #WARN_SKEW} is logged - the relay is right either way, but the host's clock is not.
 *
 * <p>A consumer may declare its own bean of this type (a test skewing a pod's clock does).
 */
@Slf4j
public class OutboxClock {

  /** How often the offset is measured again. */
  static final Duration REMEASURE = Duration.ofSeconds(30);

  /** A skew worth a warning: the host's clock is off the database's by more than this. */
  static final Duration WARN_SKEW = Duration.ofSeconds(1);

  /** Round trips per measurement; the fastest one is kept. */
  static final int SAMPLES = 5;

  /** Added to half the fastest round trip: an offset within the sum is measurement noise. */
  static final Duration UNCERTAINTY_FLOOR = Duration.ofMillis(1);

  private final Clock jvm;
  private final Supplier<Instant> database;
  private volatile long offsetNanos;
  private volatile long measuredAtNanos;
  private volatile boolean measured;
  private volatile boolean warned;

  /**
   * @param jvm the pod's own clock (the system UTC clock in production)
   * @param database the database's current instant - one round trip; null = no database: the JVM
   *     clock as it is
   */
  public OutboxClock(Clock jvm, Supplier<Instant> database) {
    this.jvm = jvm;
    this.database = database;
  }

  /** The JVM clock alone - for code that has no database to measure against. */
  public static OutboxClock system() {
    return new OutboxClock(Clock.systemUTC(), null);
  }

  /** The database's current instant, as this pod knows it. */
  public Instant instant() {
    return jvm.instant().plusNanos(offsetNanos);
  }

  /** {@link #instant()} at UTC. */
  public OffsetDateTime now() {
    return OffsetDateTime.ofInstant(instant(), ZoneOffset.UTC);
  }

  /** The measured offset: database minus JVM (zero until the first measurement). */
  public Duration offset() {
    return Duration.ofNanos(offsetNanos);
  }

  /**
   * Measures the offset now - one round trip to the database.
   *
   * @return whether it was measured (false: no database, or the round trip failed)
   */
  public boolean measure() {
    if (database == null) {
      return false;
    }
    try {
      long fastest = Long.MAX_VALUE;
      long measuredOffset = 0;
      for (int sample = 0; sample < SAMPLES; sample++) {
        long started = System.nanoTime();
        Instant before = jvm.instant();
        Instant databaseNow = database.get();
        long roundTrip = System.nanoTime() - started;
        if (roundTrip < fastest) {
          fastest = roundTrip;
          measuredOffset =
              Duration.between(before.plusNanos(roundTrip / 2), databaseNow).toNanos();
        }
      }
      long uncertainty = fastest / 2 + UNCERTAINTY_FLOOR.toNanos();
      long offset = Math.abs(measuredOffset) <= uncertainty ? 0 : measuredOffset;
      offsetNanos = offset;
      measuredAtNanos = System.nanoTime();
      measured = true;
      warnOnSkew(Duration.ofNanos(offset));
      return true;
    } catch (RuntimeException ex) {
      log.warn(
          "Outbox clock not measured against the database - it keeps the offset {}: {}",
          offset(),
          ex.toString());
      return false;
    }
  }

  /** Measures again once the last measurement is {@link #REMEASURE} old (or there was none). */
  public void measureIfDue() {
    if (!measured || System.nanoTime() - measuredAtNanos >= REMEASURE.toNanos()) {
      measure();
    }
  }

  private void warnOnSkew(Duration offset) {
    boolean skewed = offset.abs().compareTo(WARN_SKEW) > 0;
    if (skewed && !warned) {
      log.warn(
          "This host's clock is {} off the database's - the outbox keeps time by the database"
              + " (leases, retries, holds); the host's clock wants fixing",
          offset.negated());
    } else if (!skewed && warned) {
      log.info("This host's clock is back within {} of the database's", WARN_SKEW);
    }
    warned = skewed;
  }
}
