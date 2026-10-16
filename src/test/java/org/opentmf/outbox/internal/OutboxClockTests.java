package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/** The outbox's time (1.9.0): the JVM clock corrected by the offset measured to the database's. */
class OutboxClockTests {

  private static final Instant DATABASE_NOW = Instant.parse("2026-10-16T08:00:00Z");

  /** The JVM clock is fixed; the round trip (half of it is taken off) is a few microseconds. */
  private static final Duration CLOSE = Duration.ofMillis(5);

  private final ListAppender<ILoggingEvent> log = new ListAppender<>();

  OutboxClockTests() {
    log.start();
    ((Logger) LoggerFactory.getLogger(OutboxClock.class)).addAppender(log);
  }

  @AfterEach
  void detach() {
    ((Logger) LoggerFactory.getLogger(OutboxClock.class)).detachAppender(log);
  }

  private static Clock jvm(Duration skew) {
    return Clock.fixed(DATABASE_NOW.plus(skew), ZoneOffset.UTC);
  }

  @Test
  void aPodFiveMinutesAhead_keepsTheDatabasesTime_onceMeasured() {
    OutboxClock clock = new OutboxClock(jvm(Duration.ofMinutes(5)), () -> DATABASE_NOW);
    assertThat(clock.instant()).isEqualTo(DATABASE_NOW.plus(Duration.ofMinutes(5))); // unmeasured

    assertThat(clock.measure()).isTrue();

    assertThat(clock.offset()).isCloseTo(Duration.ofMinutes(-5), CLOSE);
    assertThat(clock.instant()).isCloseTo(DATABASE_NOW, within(5, ChronoUnit.MILLIS));
    assertThat(clock.now().toInstant()).isCloseTo(DATABASE_NOW, within(5, ChronoUnit.MILLIS));
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
    OutboxClock clock = new OutboxClock(jvm(Duration.ofMinutes(-5)), () -> DATABASE_NOW);

    clock.measure();

    assertThat(clock.instant()).isCloseTo(DATABASE_NOW, within(5, ChronoUnit.MILLIS));
  }

  @Test
  void aSmallSkew_isCorrected_withoutAWarning_andAWarnedSkewThatHealsIsSaidOnce() {
    AtomicReference<Instant> database = new AtomicReference<>(DATABASE_NOW.plusSeconds(3));
    OutboxClock clock = new OutboxClock(jvm(Duration.ZERO), database::get);

    clock.measure(); // 3 s: warned
    database.set(DATABASE_NOW.plusMillis(200));
    clock.measure(); // healed: said once
    clock.measure(); // still fine: nothing

    assertThat(clock.offset()).isCloseTo(Duration.ofMillis(200), CLOSE);
    assertThat(log.list).filteredOn(e -> e.getLevel() == Level.WARN).hasSize(1);
    assertThat(log.list)
        .filteredOn(e -> e.getLevel() == Level.INFO)
        .singleElement()
        .satisfies(e -> assertThat(e.getFormattedMessage()).contains("back within"));
  }

  @Test
  void aFailedMeasurement_keepsTheLastOffset() {
    AtomicInteger calls = new AtomicInteger();
    OutboxClock clock =
        new OutboxClock(
            jvm(Duration.ofSeconds(10)),
            () -> {
              if (calls.incrementAndGet() > OutboxClock.SAMPLES) {
                throw new IllegalStateException("database gone");
              }
              return DATABASE_NOW;
            });

    assertThat(clock.measure()).isTrue();
    assertThat(clock.measure()).isFalse();

    assertThat(clock.offset()).isCloseTo(Duration.ofSeconds(-10), CLOSE);
    assertThat(log.list)
        .anySatisfy(e -> assertThat(e.getFormattedMessage()).contains("keeps the offset PT-"));
  }

  @Test
  void measureIfDue_measuresTheFirstTime_thenOnlyAfterTheRemeasurePeriod() {
    AtomicInteger calls = new AtomicInteger();
    OutboxClock clock =
        new OutboxClock(
            jvm(Duration.ZERO),
            () -> {
              calls.incrementAndGet();
              return DATABASE_NOW;
            });

    clock.measureIfDue();
    clock.measureIfDue();

    assertThat(calls).hasValue(OutboxClock.SAMPLES); // one measurement: its round trips
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
    OutboxClock clock =
        new OutboxClock(
            Clock.systemUTC(),
            () -> {
              Instant now = Instant.now();
              if (calls.incrementAndGet() == 1) {
                // answered at once, the answer delayed 150 ms on the way back
                LockSupport.parkNanos(Duration.ofMillis(150).toNanos());
              }
              return now;
            });

    assertThat(clock.measure()).isTrue();

    assertThat(clock.offset()).isZero();
    assertThat(log.list).noneMatch(e -> e.getLevel() == Level.WARN);
  }

  /** A database every round trip of which takes ~20 ms, its clock answering half-way. */
  private static OutboxClock slowTrips(Duration skew) {
    return new OutboxClock(
        Clock.systemUTC(),
        () -> {
          LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
          Instant halfWay = Instant.now().plus(skew);
          LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
          return halfWay;
        });
  }

  /** The offset is taken against the JVM's instant HALF-way through the round trip. */
  @Test
  void theOffset_isMeasuredHalfWayThroughTheRoundTrip() {
    OutboxClock clock = slowTrips(Duration.ofMinutes(5));

    clock.measure();

    assertThat(clock.offset()).isCloseTo(Duration.ofMinutes(5), Duration.ofMillis(6));
  }

  /** An offset beyond half the fastest round trip (plus a millisecond) is no noise: applied. */
  @Test
  void anOffsetBeyondHalfTheRoundTrip_isApplied_withinIt_isNone() {
    OutboxClock beyond = slowTrips(Duration.ofMillis(25)); // uncertainty ~11 ms
    beyond.measure();
    assertThat(beyond.offset()).isBetween(Duration.ofMillis(18), Duration.ofMillis(32));

    OutboxClock within = slowTrips(Duration.ofMillis(6));
    within.measure();
    assertThat(within.offset()).isZero();
  }

  @Test
  void anOffsetAboveTheUncertainty_isApplied() {
    OutboxClock clock = new OutboxClock(Clock.systemUTC(), () -> Instant.now().plusMillis(40));

    clock.measure();

    assertThat(clock.offset()).isBetween(Duration.ofMillis(35), Duration.ofMillis(45));
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
