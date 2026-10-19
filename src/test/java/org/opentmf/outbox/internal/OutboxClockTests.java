package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * The outbox's time (1.9.0): the JVM clock corrected by the offset measured to the database's.
 *
 * <p>Every case runs on SIMULATED time: the JVM clock, the round-trip ticker and the database's
 * answer all read one nanosecond count that only a simulated round trip advances - so each offset
 * is exact, whatever the host's scheduling (a case on real time was red under load: one stalled
 * round trip moved its offset past the tolerance).
 */
class OutboxClockTests {

  private static final Instant JVM_START = Instant.parse("2026-10-16T08:00:00Z");

  private final AtomicLong elapsed = new AtomicLong(); // simulated nanoseconds

  private final Clock jvm =
      new Clock() {
        @Override
        public ZoneId getZone() {
          return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
          throw new UnsupportedOperationException();
        }

        @Override
        public Instant instant() {
          return jvmNow();
        }
      };

  private final ListAppender<ILoggingEvent> log = new ListAppender<>();

  OutboxClockTests() {
    log.start();
    ((Logger) LoggerFactory.getLogger(OutboxClock.class)).addAppender(log);
  }

  @AfterEach
  void detach() {
    ((Logger) LoggerFactory.getLogger(OutboxClock.class)).detachAppender(log);
  }

  private Instant jvmNow() {
    return JVM_START.plusNanos(elapsed.get());
  }

  private void pass(Duration duration) {
    elapsed.addAndGet(duration.toNanos());
  }

  /** One round trip: {@code out} to the database, which answers its clock, {@code back}. */
  private Supplier<Instant> database(Supplier<Duration> skew, Duration out, Duration back) {
    return () -> {
      pass(out);
      Instant answer = jvmNow().plus(skew.get());
      pass(back);
      return answer;
    };
  }

  /** A database whose clock is {@code skew} off the JVM's (database minus JVM), 1 ms each way. */
  private Supplier<Instant> database(Duration skew) {
    return database(() -> skew, Duration.ofMillis(1), Duration.ofMillis(1));
  }

  private OutboxClock clock(Supplier<Instant> database) {
    return new OutboxClock(jvm, database, elapsed::get);
  }

  @Test
  void aPodFiveMinutesAhead_keepsTheDatabasesTime_onceMeasured() {
    OutboxClock clock = clock(database(Duration.ofMinutes(-5)));
    assertThat(clock.instant()).isEqualTo(jvmNow()); // unmeasured: the JVM's own

    assertThat(clock.measure()).isTrue();

    assertThat(clock.offset()).isEqualTo(Duration.ofMinutes(-5));
    assertThat(clock.instant()).isEqualTo(jvmNow().minus(Duration.ofMinutes(5)));
    assertThat(clock.now().toInstant()).isEqualTo(jvmNow().minus(Duration.ofMinutes(5)));
    assertThat(clock.now().getOffset()).isEqualTo(ZoneOffset.UTC);
    assertThat(log.list)
        .anySatisfy(
            e -> {
              assertThat(e.getLevel()).isEqualTo(Level.WARN);
              assertThat(e.getFormattedMessage()).contains("PT5M").contains("off the database's");
            });
  }

  @Test
  void aPodFiveMinutesBehind_keepsTheDatabasesTime_onceMeasured() {
    OutboxClock clock = clock(database(Duration.ofMinutes(5)));

    clock.measure();

    assertThat(clock.instant()).isEqualTo(jvmNow().plus(Duration.ofMinutes(5)));
  }

  @Test
  void aSmallSkew_isCorrected_withoutAWarning_andAWarnedSkewThatHealsIsSaidOnce() {
    AtomicReference<Duration> skew = new AtomicReference<>(Duration.ofSeconds(3));
    OutboxClock clock = clock(database(skew::get, Duration.ofMillis(1), Duration.ofMillis(1)));

    clock.measure(); // 3 s: warned
    skew.set(Duration.ofMillis(200));
    clock.measure(); // healed: said once
    clock.measure(); // still fine: nothing

    assertThat(clock.offset()).isEqualTo(Duration.ofMillis(200));
    assertThat(log.list).filteredOn(e -> e.getLevel() == Level.WARN).hasSize(1);
    assertThat(log.list)
        .filteredOn(e -> e.getLevel() == Level.INFO)
        .singleElement()
        .satisfies(e -> assertThat(e.getFormattedMessage()).contains("back within"));
  }

  @Test
  void aFailedMeasurement_keepsTheLastOffset() {
    AtomicInteger calls = new AtomicInteger();
    Supplier<Instant> database = database(Duration.ofSeconds(-10));
    OutboxClock clock =
        clock(
            () -> {
              if (calls.incrementAndGet() > OutboxClock.SAMPLES) {
                throw new IllegalStateException("database gone");
              }
              return database.get();
            });

    assertThat(clock.measure()).isTrue();
    assertThat(clock.measure()).isFalse();

    assertThat(clock.offset()).isEqualTo(Duration.ofSeconds(-10));
    assertThat(log.list)
        .anySatisfy(e -> assertThat(e.getFormattedMessage()).contains("keeps the offset PT-"));
  }

  @Test
  void measureIfDue_measuresTheFirstTime_thenOnlyAfterTheRemeasurePeriod() {
    AtomicInteger calls = new AtomicInteger();
    Supplier<Instant> database = database(() -> Duration.ZERO, Duration.ZERO, Duration.ZERO);
    OutboxClock clock =
        clock(
            () -> {
              calls.incrementAndGet();
              return database.get();
            });

    clock.measureIfDue();
    clock.measureIfDue();
    assertThat(calls).hasValue(OutboxClock.SAMPLES); // one measurement: its round trips

    pass(OutboxClock.REMEASURE.minusNanos(1));
    clock.measureIfDue();
    assertThat(calls).hasValue(OutboxClock.SAMPLES); // not yet due

    pass(Duration.ofNanos(1));
    clock.measureIfDue();
    assertThat(calls).hasValue(2 * OutboxClock.SAMPLES); // due: measured again
  }

  /**
   * A pod on the database's clock, one slow round trip among the samples (a proxy, a loaded host):
   * the fastest trip is kept and the offset left within its uncertainty is none - the clock is
   * never "corrected" by the noise of its own measurement (the gate's K3 saw rows go 50-85 ms
   * before their hold when one slow trip decided the offset).
   */
  @Test
  void theSameClock_withASlowRoundTripAmongTheSamples_isLeftUncorrected() {
    AtomicInteger calls = new AtomicInteger();
    Supplier<Instant> slow = database(() -> Duration.ZERO, Duration.ZERO, Duration.ofMillis(150));
    // the fast trips are lopsided too: 0.2 ms out, 0.8 ms back - measured -0.3 ms, within 1.5 ms
    Supplier<Instant> fast =
        database(() -> Duration.ZERO, Duration.ofNanos(200_000), Duration.ofNanos(800_000));
    OutboxClock clock = clock(() -> calls.incrementAndGet() == 1 ? slow.get() : fast.get());

    assertThat(clock.measure()).isTrue();

    assertThat(clock.offset()).isZero();
    assertThat(log.list).noneMatch(e -> e.getLevel() == Level.WARN);
  }

  /** A database every round trip of which takes 20 ms, its clock answering half-way. */
  private OutboxClock slowTrips(Duration skew) {
    return clock(database(() -> skew, Duration.ofMillis(10), Duration.ofMillis(10)));
  }

  /** The offset is taken against the JVM's instant HALF-way through the round trip. */
  @Test
  void theOffset_isMeasuredHalfWayThroughTheRoundTrip() {
    OutboxClock clock = slowTrips(Duration.ofMinutes(5));

    clock.measure();

    assertThat(clock.offset()).isEqualTo(Duration.ofMinutes(5));
  }

  /**
   * An offset beyond half the fastest round trip plus a millisecond is no noise: applied. On 20 ms
   * trips the uncertainty is 10 ms + 1 ms: 11 ms is none, a nanosecond more is applied.
   */
  @Test
  void anOffsetBeyondHalfTheRoundTrip_isApplied_withinIt_isNone() {
    OutboxClock beyond = slowTrips(Duration.ofMillis(25));
    beyond.measure();
    assertThat(beyond.offset()).isEqualTo(Duration.ofMillis(25));

    OutboxClock within = slowTrips(Duration.ofMillis(6));
    within.measure();
    assertThat(within.offset()).isZero();

    OutboxClock atTheEdge = slowTrips(Duration.ofMillis(11));
    atTheEdge.measure();
    assertThat(atTheEdge.offset()).isZero();

    Duration justPast = Duration.ofMillis(11).plusNanos(1);
    OutboxClock past = slowTrips(justPast);
    past.measure();
    assertThat(past.offset()).isEqualTo(justPast);

    OutboxClock behind = slowTrips(justPast.negated()); // the edge holds on both sides
    behind.measure();
    assertThat(behind.offset()).isEqualTo(justPast.negated());
  }

  @Test
  void anOffsetAboveTheUncertainty_isApplied() {
    OutboxClock clock = clock(database(() -> Duration.ofMillis(40), Duration.ZERO, Duration.ZERO));

    clock.measure();

    assertThat(clock.offset()).isEqualTo(Duration.ofMillis(40)); // instant trips: 1 ms floor
  }

  @Test
  void withoutADatabase_theJvmClockAsItIs() {
    OutboxClock clock = OutboxClock.system();
    assertThat(clock.measure()).isFalse();
    clock.measureIfDue();
    assertThat(clock.offset()).isZero();
    assertThat(Duration.between(clock.instant(), Instant.now()).abs())
        .isLessThan(Duration.ofSeconds(1));
  }
}
