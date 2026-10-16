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
              if (calls.incrementAndGet() > 1) {
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

    assertThat(calls).hasValue(1);
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
