package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import jakarta.persistence.Column;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opentmf.outbox.OutboxBooking;
import org.opentmf.outbox.OutboxBooking.Outcome;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxFailureText;
import org.opentmf.outbox.OutboxLeaseLapsedException;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxPublisher.ExhaustionOutcome;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.opentmf.outbox.OutboxRelayedListener;
import org.opentmf.outbox.OutboxRowView;
import org.opentmf.outbox.OutboxSendError;
import org.opentmf.outbox.TerminalOutboxException;
import org.opentmf.outbox.fixture.TestTracing;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionOperations;

/**
 * The lease mechanics on a fake table (the real claim query and the real transactions are the
 * ITs' job): the claim stamps and takes rows by lane, slot and ordering key; each outcome is
 * booked guarded on the stamp, with the publisher's hook for every outcome; a booking that throws
 * ROLLS BACK (the fake transaction restores the rows it saw); ORDERED leases are renewed one row
 * ahead.
 */
class OutboxRelayWorkerTests {

  private final Map<Long, OutboxEvent> table = new LinkedHashMap<>();

  /** Rows another pod's claim holds {@code FOR UPDATE} right now: the claim SKIPS them. */
  private final Set<Long> lockedElsewhere = new HashSet<>();
  private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
  private final OutboxPublisher publisher = mock(OutboxPublisher.class, CALLS_REAL_METHODS);
  private final OutboxProperties properties = new OutboxProperties();
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final List<OutboxRelayedListener> listeners = new ArrayList<>();
  private final List<OutboxBooking> bookings = new CopyOnWriteArrayList<>();
  /** Two slots, a hand-off queue of two: a claim takes at most two CONCURRENT rows. */
  private final OutboxConcurrentLane lane =
      fast(new OutboxConcurrentLane(2, 2, false, Duration.ofSeconds(5)));
  /** The ORDERED lane's one thread (1.8.0): a batch is sent off the calling thread. */
  private final OutboxOrderedLane orderedLane = new OutboxOrderedLane(Duration.ofSeconds(5));

  /**
   * What the ORDERED lane logs: a batch that throws is caught and logged on the lane's thread, so
   * no test would see it otherwise - every test ends with none.
   */
  private final ListAppender<ILoggingEvent> orderedLaneLog = new ListAppender<>();
  private int committed;
  private int rolledBack;
  private boolean failCommits;

  /** Commits = keep; a thrown callback = restore every row to its state before the callback. */
  private final TransactionOperations tx =
      new TransactionOperations() {
        @Override
        public <T> T execute(TransactionCallback<T> action) {
          Map<Long, OutboxEvent> before = new LinkedHashMap<>();
          table.forEach((id, row) -> before.put(id, copy(row)));
          try {
            T result = action.doInTransaction(null);
            if (failCommits) {
              throw new IllegalStateException("database gone at commit");
            }
            committed++;
            return result;
          } catch (RuntimeException | Error ex) { // as TransactionTemplate: an Error rolls back too
            rolledBack++;
            before.forEach((id, row) -> BeanUtils.copyProperties(row, table.get(id)));
            throw ex;
          }
        }
      };

  private final OutboxRelayWorker worker = workerOn(lane);

  /**
   * A lane that has measured a fast send (1.9.0): it claims ahead of its slots up to its capacity,
   * as every lane did before - these tests are about that regime.
   */
  private static OutboxConcurrentLane fast(OutboxConcurrentLane lane) {
    lane.sendTook(Duration.ofMillis(1).toNanos());
    return lane;
  }

  private OutboxRelayWorker workerOn(OutboxConcurrentLane onLane) {
    return workerOn(onLane, orderedLane, OutboxSendHeartbeatFixture.idle());
  }

  private OutboxRelayWorker workerOn(
      OutboxConcurrentLane onLane, OutboxOrderedLane onOrdered, OutboxSendHeartbeat heartbeat) {
    return new OutboxRelayWorker(repository,
        new OutboxPublisherRouter(List.of(publisher)),
        new OutboxBackoff(properties),
        new OutboxMetrics(registry, repository, Duration.ofSeconds(15)),
        properties,
        listeners,
        tx,
        tx,
        onLane,
        onOrdered,
            strictlyAdvancing(),
            heartbeat);
  }

  /**
   * The system clock, each read at least a microsecond - the stamps' precision - after the one
   * before: never behind the system's time, ahead of it only while reads come faster. Two stamps
   * of one pass (a claim and the renewal after it) are thus never the same microsecond, on any
   * host's clock tick; on the system clock alone a fast host's mocked pass stamped both in one.
   */
  private static OutboxClock strictlyAdvancing() {
    Clock system = Clock.systemUTC();
    AtomicReference<Instant> last = new AtomicReference<>(Instant.MIN);
    return new OutboxClock(
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
            return last.updateAndGet(
                previous -> {
                  Instant now = system.instant();
                  Instant next = previous.plus(1, ChronoUnit.MICROS);
                  return now.isBefore(next) ? next : now;
                });
          }
        },
        null);
  }

  OutboxRelayWorkerTests() {
    orderedLaneLog.start();
    ((Logger) LoggerFactory.getLogger(OutboxOrderedLane.class)).addAppender(orderedLaneLog);
    when(publisher.supports(any())).thenReturn(true);
    doThrow(new AssertionError("unstubbed publish")).when(publisher).publish(any());
    doReturn(null).when(publisher).deliver(any()); // delivered, no result - per test overridden
    // the two lane claims, emulated on the fake table (the real SQL is the ITs' job)
    when(repository.claimOrdered(any(), anyInt()))
        .thenAnswer(
            inv -> {
              OffsetDateTime now = inv.getArgument(0);
              int limit = inv.getArgument(1);
              return table.values().stream()
                  .filter(e -> e.getLane() != Lane.CONCURRENT && claimable(e, now))
                  .limit(limit)
                  .toList();
            });
    when(repository.claimConcurrent(any(), any(), anyInt()))
        .thenAnswer(
            inv ->
                claimConcurrent(
                    inv.getArgument(0), inv.getArgument(1), null, inv.getArgument(2), true));
    when(repository.claimConcurrentWrap(any(), any(), anyInt()))
        .thenAnswer(
            inv ->
                claimConcurrent(
                    inv.getArgument(0), "", inv.getArgument(1), inv.getArgument(2), false));
    when(repository.concurrentHeadKeys(any(), any(), anyInt()))
        .thenAnswer(
            inv ->
                heads(inv.getArgument(0), inv.getArgument(1), null, inv.getArgument(2)).stream()
                    .map(OutboxEvent::getOrderingKey)
                    .toList());
    // the cross-pod guard: every key lock is free, the re-check keeps every candidate - unless a
    // test says otherwise
    when(repository.tryLockKeys(anyInt(), any())).thenAnswer(inv -> keysOf(inv.getArgument(1)));
    when(repository.recheckKeyedHeads(any(), any()))
        .thenAnswer(inv -> idsOf(inv.getArgument(1)));
    // the set-based stamp and the one-statement RELAYED booking, on the fake table
    when(repository.stampLeases(any(), any()))
        .thenAnswer(
            inv -> {
              OffsetDateTime stamp = inv.getArgument(0);
              List<Long> ids = idsOf(inv.getArgument(1));
              ids.forEach(
                  id -> {
                    table.get(id).setClaimedUntil(stamp);
                    table.get(id).setNextAttemptOn(stamp);
                    table.get(id).setSendingSince(null);
                  });
              return ids.size();
            });
    when(repository.bookRelayed(anyLong(), any(), any()))
        .thenAnswer(
            inv -> {
              Optional<OutboxEvent> held = leased(inv.getArgument(0), inv.getArgument(1));
              held.ifPresent(
                  row -> {
                    row.setClaimedUntil(null);
                    row.setSendingSince(null);
                    row.setRelayedOn(inv.getArgument(2));
                    row.setNextAttemptOn(inv.getArgument(2));
                  });
              return held;
            });
    when(repository.lockLeased(anyLong(), any()))
        .thenAnswer(inv -> leased(inv.getArgument(0), inv.getArgument(1)));
    doBookingsRecorded();
  }

  private void doBookingsRecorded() {
    doAnswer(
            inv -> {
              bookings.add(inv.getArgument(1));
              return null;
            })
        .when(publisher)
        .onBooked(any(), any());
  }

  @AfterEach
  void closeLane() {
    lane.close();
    orderedLane.close();
    ((Logger) LoggerFactory.getLogger(OutboxOrderedLane.class)).detachAppender(orderedLaneLog);
    assertThat(orderedLaneLog.list)
        .as("no ORDERED batch threw on the lane's thread")
        .noneMatch(event -> event.getLevel() == Level.ERROR);
  }

  /** The row under the lease {@code stamp}, as the lease guard reads it. */
  private Optional<OutboxEvent> leased(long id, OffsetDateTime stamp) {
    return Optional.ofNullable(table.get(id))
        .filter(e -> Objects.equals(e.getClaimedUntil(), stamp));
  }

  /** The keys of a PostgreSQL array literal of double-quoted elements such as {@code {"a","b"}}. */
  private static List<String> keysOf(String literal) {
    String inner = literal.substring(1, literal.length() - 1);
    return inner.isEmpty()
        ? List.of()
        : Stream.of(inner.substring(1, inner.length() - 1).split("\",\"")).toList();
  }

  /** The ids of a PostgreSQL array literal such as {@code {7,12}}. */
  private static List<Long> idsOf(String literal) {
    String inner = literal.substring(1, literal.length() - 1);
    return inner.isEmpty()
        ? List.of()
        : Stream.of(inner.split(",")).map(Long::valueOf).toList();
  }

  private static boolean claimable(OutboxEvent e, OffsetDateTime now) {
    return e.getRelayedOn() == null
        && e.getCancelledOn() == null
        && e.getParkedOn() == null
        && (e.getReleaseAt() == null || !e.getReleaseAt().isAfter(now))
        && !e.getNextAttemptOn().isAfter(now)
        && (e.getClaimedUntil() == null || !e.getClaimedUntil().isAfter(now));
  }

  /**
   * {@link OutboxClaimSql#CONCURRENT} / {@code CONCURRENT_WRAP} on the fake table: keys
   * round-robin above {@code after} / up to {@code until}, each key not in flight contributing its
   * first due row, until {@code limit} heads; rows without a key in id order (first pass only);
   * the union in id order, {@code limit} rows - less the rows another pod holds locked ({@code FOR
   * UPDATE SKIP LOCKED} drops them after the heads were chosen, so the pass comes back short).
   */
  private List<OutboxEvent> claimConcurrent(
      OffsetDateTime now, String after, String until, int limit, boolean withUnkeyed) {
    List<OutboxEvent> heads = heads(now, after, until, limit);
    List<OutboxEvent> unkeyed =
        pendingConcurrent().stream()
            .filter(e -> withUnkeyed && e.getOrderingKey() == null && claimable(e, now))
            .limit(limit)
            .toList();
    return Stream.concat(heads.stream(), unkeyed.stream())
        .sorted(Comparator.comparing(OutboxEvent::getId))
        .limit(limit)
        .filter(e -> !lockedElsewhere.contains(e.getId()))
        .toList();
  }

  private List<OutboxEvent> pendingConcurrent() {
    return table.values().stream()
        .filter(
            e ->
                e.getLane() == Lane.CONCURRENT
                    && e.getRelayedOn() == null
                    && e.getCancelledOn() == null
                    && e.getParkedOn() == null)
        .toList();
  }

  /** The keyed heads of a pass, before any row lock: each key not in flight, its first due row. */
  private List<OutboxEvent> heads(OffsetDateTime now, String after, String until, int limit) {
    List<OutboxEvent> pendingConcurrent = pendingConcurrent();
    Set<String> inFlight =
        table.values().stream()
            .filter(
                e ->
                    e.getLane() == Lane.CONCURRENT
                        && e.getRelayedOn() == null
                        && e.getOrderingKey() != null
                        && e.getClaimedUntil() != null
                        && e.getClaimedUntil().isAfter(now))
            .map(OutboxEvent::getOrderingKey)
            .collect(Collectors.toSet());
    List<OutboxEvent> heads = new ArrayList<>();
    pendingConcurrent.stream()
        .map(OutboxEvent::getOrderingKey)
        .filter(Objects::nonNull)
        .filter(k -> k.compareTo(after) > 0 && (until == null || k.compareTo(until) <= 0))
        .distinct()
        .sorted()
        .filter(k -> !inFlight.contains(k))
        .forEach(
            k ->
                pendingConcurrent.stream()
                    .filter(e -> k.equals(e.getOrderingKey()) && claimable(e, now))
                    .findFirst()
                    .filter(head -> heads.size() < limit)
                    .ifPresent(heads::add));
    return heads;
  }

  private static OutboxEvent copy(OutboxEvent row) {
    OutboxEvent copy = new OutboxEvent();
    BeanUtils.copyProperties(row, copy);
    return copy;
  }

  private OutboxEvent pending(long id, int attempts) {
    OutboxEvent event = new OutboxEvent();
    event.setId(id);
    event.setDestination("comm.delivery.v1");
    event.setAttempts(attempts);
    event.setNextAttemptOn(OffsetDateTime.now().minusSeconds(1));
    table.put(id, event);
    return event;
  }

  /** A CONCURRENT row as the writer stamps it at append. */
  private OutboxEvent concurrent(long id, String key) {
    OutboxEvent event = pending(id, 0);
    event.setLane(Lane.CONCURRENT);
    event.setOrderingKey(key);
    return event;
  }

  /** In flight elsewhere: a live lease, next_attempt_on moved with it. */
  private static void leasedElsewhere(OutboxEvent event) {
    OffsetDateTime lease = OffsetDateTime.now().plusMinutes(1);
    event.setClaimedUntil(lease);
    event.setNextAttemptOn(lease);
  }

  private double counter(String name) {
    return registry.find(name).counters().stream().mapToDouble(Counter::count).sum();
  }

  /** One batch over both lanes, then wait for both lanes to send everything they hold. */
  private int relay() {
    int claimed = worker.relayBatch(true).claimed();
    orderedIdle();
    idle(lane, 2);
    return claimed;
  }

  private void orderedIdle() {
    await().atMost(Duration.ofSeconds(5)).until(() -> !orderedLane.busy());
  }

  private static void idle(OutboxConcurrentLane onLane, int slots) {
    await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> onLane.available() == slots && onLane.queued() == 0);
  }

  // ------------------------------------------------------------ the ORDERED lane, 1.2.x policy

  @Test
  void successfulRelay_stampsRelayedOn_clearsTheLease_booksTheMetrics() {
    OutboxEvent event = pending(1L, 0);

    int claimed = relay();

    assertThat(claimed).isEqualTo(1);
    assertThat(event.getRelayedOn()).isNotNull();
    assertThat(event.getClaimedUntil()).isNull(); // the booking released the lease
    assertThat(event.getParkedOn()).isNull();
    assertThat(
            registry
                .get(OutboxMetrics.RELAYED)
                .tag(OutboxMetrics.TAG_DESTINATION, "comm.delivery.v1")
                .counter()
                .count())
        .isEqualTo(1d);
    assertThat(registry.get(OutboxMetrics.ATTEMPTS).summary().totalAmount()).isEqualTo(1d);
    // the ORDERED claim, the CONCURRENT claim (a slot was free), one booking: three SHORT
    // transactions, the send in none
    assertThat(committed).isEqualTo(3);
  }

  @Test
  void failure_booksAttemptAndBackoff_aFailedRowDoesNotStopTheBatch() {
    OutboxEvent failing = pending(1L, 0);
    OutboxEvent fine = pending(2L, 0);
    doThrow(new RuntimeException("broker down")).when(publisher).deliver(failing);

    relay();

    assertThat(failing.getRelayedOn()).isNull();
    assertThat(failing.getAttempts()).isEqualTo(1);
    // EXACT format: SimpleName + message (toString would carry the package prefix)
    assertThat(failing.getLastError()).isEqualTo("RuntimeException: broker down");
    assertThat(failing.getNextAttemptOn()).isAfter(OffsetDateTime.now());
    assertThat(failing.getClaimedUntil()).isNull();
    assertThat(fine.getRelayedOn()).isNotNull();
  }

  @Test
  void theDefaultDeliver_callsPublish() {
    OutboxPublisher plain = mock(OutboxPublisher.class, CALLS_REAL_METHODS);
    doThrow(new RuntimeException("from publish")).when(plain).publish(any());
    OutboxEvent event = pending(1L, 0);

    assertThatThrownBy(() -> plain.deliver(event))
        .hasMessage("from publish");
    assertThat(plain.lane(event)).isEqualTo(Lane.ORDERED);
    assertThat(plain.orderingKey(event)).isNull();
    assertThat(plain.lease(event)).isNull();
    assertThat(plain.onExhausted(event)).isEqualTo(ExhaustionOutcome.PARK);
  }

  @Test
  void relayedListeners_runInsideTheBooking_withTheStampVisible() {
    OutboxEvent event = pending(1L, 0);
    List<Object> seenRelayedOn = new ArrayList<>();
    listeners.add(e -> seenRelayedOn.add(e.getRelayedOn()));

    relay();

    assertThat(seenRelayedOn).hasSize(1);
    assertThat(seenRelayedOn.get(0)).isNotNull();
    assertThat(event.getRelayedOn()).isNotNull();
  }

  @Test
  void aThrowingListener_rollsTheStampBack_andBooksAnOrdinaryFailure() {
    OutboxEvent event = pending(1L, 0);
    listeners.add(
        e -> {
          e.setReference("written by the listener before it threw");
          throw new IllegalStateException("bookkeeping refused");
        });

    relay();

    assertThat(rolledBack).isEqualTo(1);
    assertThat(event.getRelayedOn()).isNull();
    assertThat(event.getReference()).isNull(); // the listener's write rolled back with the stamp
    assertThat(event.getAttempts()).isEqualTo(1);
    assertThat(event.getLastError()).isEqualTo("IllegalStateException: bookkeeping refused");
    assertThat(registry.find(OutboxMetrics.RELAYED).counters()).isEmpty();
    // the hook saw the refused success, then the failure it became
    assertThat(bookings)
        .extracting(OutboxBooking::outcome)
        .containsExactly(Outcome.RELAYED, Outcome.RETRY);
  }

  /**
   * THE named regression for the scheduled-send hold: a delivery failure's backoff reschedules
   * {@code next_attempt_on} and must NEVER move {@code release_at} - repurposing the hold as
   * the retry slot would release a scheduled send early on its first failure.
   */
  @Test
  void backoff_neverTouchesTheReleaseHold() throws NoSuchFieldException {
    OffsetDateTime hold = OffsetDateTime.now().minusSeconds(1); // released, so claimable
    OutboxEvent event = pending(1L, 0);
    event.setReleaseAt(hold);
    doThrow(new RuntimeException("broker down")).when(publisher).deliver(event);

    relay();

    assertThat(event.getNextAttemptOn()).isAfter(OffsetDateTime.now()); // backoff booked...
    assertThat(event.getReleaseAt()).isEqualTo(hold); // ...the hold untouched
    assertThat(OutboxEvent.class.getDeclaredField("releaseAt").getAnnotation(Column.class))
        .extracting(Column::updatable)
        .isEqualTo(false);
  }

  @Test
  void theFinalFailedAttempt_parksTheRow_byStampingParkedOn() {
    OutboxEvent event = pending(1L, properties.getMaxAttempts() - 1);
    doThrow(new RuntimeException("still down")).when(publisher).deliver(event);

    relay();

    assertThat(event.getAttempts()).isEqualTo(properties.getMaxAttempts());
    assertThat(event.getParkedOn()).isNotNull();
    assertThat(event.getRelayedOn()).isNull();
    assertThat(bookings)
        .singleElement()
        .satisfies(
            b -> {
              assertThat(b.outcome()).isEqualTo(Outcome.EXHAUSTED);
              assertThat(b.exhaustion()).isEqualTo(ExhaustionOutcome.PARK);
              assertThat(b.failure()).hasMessage("still down");
            });
  }

  @Test
  void aPublishersOwnBudgetAndBackoff_areHonoured() {
    when(publisher.maxAttempts(any())).thenReturn(3);
    when(publisher.backoff(any(), any(Integer.class))).thenReturn(Duration.ofHours(5));
    OutboxEvent retrying = pending(1L, 0);
    OutboxEvent lastChance = pending(2L, 2); // library max is 10 - the publisher says 3
    doThrow(new RuntimeException("hub 503")).when(publisher).deliver(any());

    relay();

    assertThat(retrying.getParkedOn()).isNull();
    assertThat(retrying.getNextAttemptOn()).isAfter(OffsetDateTime.now().plusHours(4));
    assertThat(lastChance.getAttempts()).isEqualTo(3);
    assertThat(lastChance.getParkedOn()).isNotNull();
  }

  @Test
  void dropOutcome_stampsRelayedOn_firesNoListener_countsDroppedNotRelayed() {
    when(publisher.maxAttempts(any())).thenReturn(1);
    when(publisher.onExhausted(any())).thenReturn(ExhaustionOutcome.DROP);
    OutboxEvent event = pending(1L, 0);
    doThrow(new RuntimeException("hub 410 gone")).when(publisher).deliver(event);
    List<Long> listened = new ArrayList<>();
    listeners.add(e -> listened.add(e.getId()));

    relay();

    assertThat(event.getRelayedOn()).isNotNull();
    assertThat(event.getParkedOn()).isNull();
    assertThat(event.getLastError()).isEqualTo("RuntimeException: hub 410 gone");
    assertThat(listened).isEmpty();
    assertThat(counter(OutboxMetrics.DROPPED)).isEqualTo(1d);
    assertThat(counter(OutboxMetrics.RELAYED)).isZero();
    assertThat(bookings)
        .extracting(OutboxBooking::exhaustion)
        .containsExactly(ExhaustionOutcome.DROP);
  }

  @Test
  void aTerminalException_reachesTheExhaustionOutcomeImmediately() {
    OutboxEvent event = pending(1L, 0);
    doThrow(new TerminalOutboxException("400 bad request - retrying is pointless"))
        .when(publisher)
        .deliver(event);

    relay();

    assertThat(event.getAttempts()).isEqualTo(1);
    assertThat(event.getParkedOn()).isNotNull();
    assertThat(event.getLastError()).contains("retrying is pointless");
  }

  @Test
  void anUnroutableRow_booksWithTheLibraryPolicy_inTheClaim() {
    when(publisher.supports(any())).thenReturn(false);
    OutboxEvent event = pending(1L, properties.getMaxAttempts() - 1);

    int claimed = relay();

    assertThat(claimed).isZero();
    assertThat(event.getLastError()).contains("No OutboxPublisher supports");
    assertThat(event.getParkedOn()).isNotNull();
    assertThat(event.getClaimedUntil()).isNull(); // never leased
    assertThat(bookings).isEmpty(); // no publisher, no hook
  }

  @Test
  void anUnroutableRowInFlight_isLeftToItsHolder() {
    when(publisher.supports(any())).thenReturn(false);
    OutboxEvent event = pending(1L, 0);
    leasedElsewhere(event);

    relay();

    assertThat(event.getAttempts()).isZero();
  }

  @Test
  void aMessagelessException_isDescribedByItsToString() {
    OutboxEvent event = pending(1L, 0);
    doThrow(new RuntimeException()).when(publisher).deliver(event);

    relay();

    assertThat(event.getLastError()).contains("RuntimeException");
  }

  @Test
  void anOversizedErrorMessage_isTruncatedForTheForensicColumn() {
    OutboxEvent event = pending(1L, 0);
    doThrow(new RuntimeException("x".repeat(10_000))).when(publisher).deliver(event);

    relay();

    assertThat(event.getLastError()).hasSize(OutboxRelayWorker.LAST_ERROR_MAX_LENGTH);
  }

  // ------------------------------------------------------------ the booking hook

  @Test
  void theBookingHook_receivesTheDeliveryResult_insideTheBooking_beforeTheListeners() {
    OutboxEvent event = pending(1L, 0);
    doReturn("acrm-42").when(publisher).deliver(event);
    List<String> order = new ArrayList<>();
    doAnswer(
            inv -> {
              OutboxEvent row = inv.getArgument(0);
              OutboxBooking booking = inv.getArgument(1);
              assertThat(row.getRelayedOn()).isNotNull(); // the stamp is already set
              row.setReference(String.valueOf(booking.result())); // the write rides the booking
              order.add("hook");
              return null;
            })
        .when(publisher)
        .onBooked(any(), any());
    listeners.add(e -> order.add("listener"));

    relay();

    assertThat(event.getReference()).isEqualTo("acrm-42");
    assertThat(order).containsExactly("hook", "listener");
  }

  /**
   * 1.9.0 (C5): a hook that refuses the failure booking as well - the failure is booked once more
   * WITHOUT the hook, so the attempt counts and the row's budget ends it. Up to 1.8.0 nothing was
   * booked and the row was re-sent every lease, forever.
   */
  @Test
  void aRefusingHookOnAFailure_isBookedWithoutTheHook_theAttemptCounts() {
    OutboxEvent event = pending(1L, 0);
    doThrow(new RuntimeException("receiver 503")).when(publisher).deliver(event);
    doThrow(new IllegalStateException("hook refused"))
        .when(publisher)
        .onBooked(any(), any());

    relay(); // logged, never thrown out of the pass

    assertThat(event.getAttempts()).isEqualTo(1);
    assertThat(event.getLastError()).contains("receiver 503");
    assertThat(event.getClaimedUntil()).isNull(); // booked: retried after its backoff
    assertThat(rolledBack).isEqualTo(1); // the booking with the hook
  }

  @Test
  void aRefusingHookOnTheFirstOrderedRow_isBookedWithoutIt_theBatchGoesOn() {
    OutboxEvent first = pending(1L, 0);
    OutboxEvent second = pending(2L, 0);
    doThrow(new RuntimeException("down")).when(publisher).deliver(first);
    doThrow(new IllegalStateException("hook refused"))
        .when(publisher)
        .onBooked(eq(first), any());

    relay();

    assertThat(first.getAttempts()).isEqualTo(1);
    verify(publisher).deliver(second); // the batch went on
    assertThat(second.getRelayedOn()).isNotNull();
  }

  // ------------------------------------------------------------ a lapse counts (1.9.0)

  /** A row whose previous lease lapsed while its send was under way (its pod died, or it hung). */
  private OutboxEvent lapsedWhileSending(long id, int attempts, Lane rowLane) {
    OutboxEvent event = pending(id, attempts);
    event.setLane(rowLane);
    event.setClaimedUntil(OffsetDateTime.now().minusSeconds(1));
    event.setSendingSince(OffsetDateTime.now().minusSeconds(20));
    return event;
  }

  @Test
  void aLeaseThatLapsedWhileItsSendWasUnderWay_isCountedAsAFailedAttempt_notSent() {
    for (Lane rowLane : Lane.values()) {
      table.clear();
      bookings.clear();
      OutboxEvent event = lapsedWhileSending(1L, 0, rowLane);

      relay();

      verify(publisher, never()).deliver(event);
      assertThat(event.getAttempts()).as(rowLane.name()).isEqualTo(1);
      assertThat(event.getLastError())
          .as(rowLane.name())
          .startsWith("OutboxLeaseLapsedException")
          .contains("lapsed unbooked");
      assertThat(event.getClaimedUntil()).as(rowLane.name()).isNull();
      assertThat(event.getSendingSince()).as(rowLane.name()).isNull();
      assertThat(event.getNextAttemptOn()).as(rowLane.name()).isAfter(OffsetDateTime.now());
      assertThat(bookings)
          .as(rowLane.name())
          .singleElement()
          .satisfies(
              booking -> {
                assertThat(booking.outcome()).isEqualTo(OutboxBooking.Outcome.RETRY);
                assertThat(booking.failure()).isInstanceOf(OutboxLeaseLapsedException.class);
              });
    }
  }

  @Test
  void aLapseOnTheLastAttempt_exhaustsTheRow_byItsPolicy() {
    OutboxEvent event = lapsedWhileSending(1L, properties.getMaxAttempts() - 1, Lane.CONCURRENT);

    relay();

    verify(publisher, never()).deliver(event);
    assertThat(event.getAttempts()).isEqualTo(properties.getMaxAttempts());
    assertThat(event.getParkedOn()).isNotNull(); // PARK, the default policy
    assertThat(bookings).singleElement().extracting(OutboxBooking::outcome).isEqualTo(OutboxBooking.Outcome.EXHAUSTED);
  }

  /** A lease that lapsed on a row held UNSENT (queued, behind an ORDERED head) costs nothing. */
  @Test
  void aLeaseThatLapsedOnARowHeldUnsent_costsItNothing() {
    for (Lane rowLane : Lane.values()) {
      table.clear();
      OutboxEvent event = pending(1L, 0);
      event.setLane(rowLane);
      event.setClaimedUntil(OffsetDateTime.now().minusSeconds(1)); // no sending_since

      relay();

      verify(publisher, atLeastOnce()).deliver(event);
      assertThat(event.getAttempts()).as(rowLane.name()).isZero();
      assertThat(event.getRelayedOn()).as(rowLane.name()).isNotNull();
    }
  }

  /** Every claim clears the mark: a new holder's send has not started. */
  @Test
  void theClaim_clearsTheSendingMark_onTheRowsItTakes() {
    OutboxEvent first = pending(1L, 0);
    OutboxEvent second = pending(2L, 0);
    first.setSendingSince(OffsetDateTime.now().minusMinutes(5)); // a stale mark, no lease
    doAnswer(
            inv -> {
              assertThat(first.getSendingSince()).isNull(); // cleared by the claim
              return null;
            })
        .when(publisher)
        .deliver(first);

    relay();

    assertThat(first.getAttempts()).isZero(); // no lease: nothing lapsed, nothing counted
    assertThat(second.getRelayedOn()).isNotNull();
  }

  /** The renewal right before an ORDERED send marks the send as under way. */
  @Test
  void theRenewalBeforeAnOrderedSend_marksTheSendUnderWay() {
    OutboxEvent first = pending(1L, 0);
    OutboxEvent second = pending(2L, 0);
    List<OffsetDateTime> markAtSend = new ArrayList<>();
    doAnswer(
            inv -> {
              markAtSend.add(second.getSendingSince());
              return null;
            })
        .when(publisher)
        .deliver(second);

    relay();

    assertThat(first.getRelayedOn()).isNotNull();
    assertThat(markAtSend).singleElement().isNotNull(); // renewed and marked, then sent
    assertThat(second.getSendingSince()).isNull(); // the booking cleared it
  }

  // ------------------------------------------------------------ anything thrown (1.9.0)

  @Test
  void anErrorFromThePublisher_isAFailedAttempt_onBothLanes() {
    for (Lane rowLane : Lane.values()) {
      table.clear();
      bookings.clear();
      OutboxEvent event = pending(1L, 0);
      event.setLane(rowLane);
      doThrow(new StackOverflowError("deep")).when(publisher).deliver(event);

      relay();

      assertThat(event.getAttempts()).as(rowLane.name()).isEqualTo(1);
      assertThat(event.getLastError()).as(rowLane.name()).contains("StackOverflowError: deep");
      assertThat(event.getClaimedUntil()).as(rowLane.name()).isNull();
      assertThat(bookings)
          .as(rowLane.name())
          .singleElement()
          .satisfies(
              booking ->
                  assertThat(booking.failure())
                      .isInstanceOf(OutboxSendError.class)
                      .hasCauseInstanceOf(StackOverflowError.class));
    }
  }

  @Test
  void anErrorFromARelayedListener_isAFailedAttempt() {
    OutboxEvent event = pending(1L, 0);
    listeners.add(
        e -> {
          throw new AssertionError("listener broke");
        });

    relay();

    assertThat(event.getRelayedOn()).isNull(); // the RELAYED booking rolled back
    assertThat(event.getAttempts()).isEqualTo(1);
    assertThat(event.getLastError()).contains("AssertionError: listener broke");
  }

  @Test
  void aRouterThatThrowsAnError_isBookedByTheLibraryPolicy() {
    OutboxEvent event = pending(1L, 0);
    doThrow(new LinkageError("missing class")).when(publisher).supports(event);

    relay();

    assertThat(event.getAttempts()).isEqualTo(1);
    assertThat(event.getLastError()).contains("LinkageError: missing class");
  }

  /** The bridge from a thrown {@link Throwable} to a booking's failure. */
  @Test
  void asFailure_keepsARuntimeException_wrapsAnythingElse() {
    IllegalStateException runtime = new IllegalStateException("x");
    assertThat(OutboxRelayWorker.asFailure(runtime)).isSameAs(runtime);
    Error error = new OutOfMemoryError("heap");
    assertThat(OutboxRelayWorker.asFailure(error))
        .isInstanceOf(OutboxSendError.class)
        .hasMessage("OutOfMemoryError: heap")
        .hasCause(error);
  }

  // ------------------------------------------------------------ the lease guard

  @Test
  void aLapsedLease_booksNothing_anotherHolderOwnsTheRow() {
    OutboxEvent event = pending(1L, 0);
    OffsetDateTime otherHolder = OffsetDateTime.now().plusHours(1);
    doAnswer(
            inv -> {
              event.setClaimedUntil(otherHolder); // lapsed and re-stamped while we sent
              return null;
            })
        .when(publisher)
        .deliver(event);

    relay();

    assertThat(event.getRelayedOn()).isNull();
    assertThat(event.getClaimedUntil()).isEqualTo(otherHolder);
    assertThat(bookings).isEmpty();
    assertThat(rolledBack).isZero(); // the lapse is booked as nothing - not as a refusal
    verify(repository, times(1)).bookRelayed(anyLong(), any(), any()); // guarded on the stamp
    verify(repository, never()).lockLeased(anyLong(), any()); // no failure path either
    assertThat(counter(OutboxMetrics.RELAYED)).isZero();
  }

  @Test
  void aLapsedLeaseOnFailure_booksNothingEither_andTheBatchGoesOn() {
    OutboxEvent event = pending(1L, 0);
    OutboxEvent next = pending(2L, 0);
    doAnswer(
            inv -> {
              event.setClaimedUntil(OffsetDateTime.now().plusHours(1));
              throw new RuntimeException("late failure");
            })
        .when(publisher)
        .deliver(event);

    relay();

    assertThat(event.getAttempts()).isZero();
    assertThat(next.getRelayedOn()).isNotNull(); // the lapse did not stop the ORDERED batch
    assertThat(rolledBack).isZero();
  }

  @Test
  void theStamp_isLeaseFromNow_truncatedToMicros_perLane_andADeclaredLeaseWins() {
    OutboxEvent ordered = pending(1L, 0);
    List<OffsetDateTime> stamps = new ArrayList<>();
    doAnswer(
            inv -> {
              stamps.add(inv.<OutboxEvent>getArgument(0).getClaimedUntil());
              return null;
            })
        .when(publisher)
        .deliver(any());
    OffsetDateTime before = OffsetDateTime.now(ZoneOffset.UTC);

    relay();

    OffsetDateTime stamp = stamps.get(0);
    assertThat(stamp)
        .isEqualTo(stamp.truncatedTo(ChronoUnit.MICROS))
        .isBetween(before.plusSeconds(14), before.plusSeconds(16)); // ordered lease 15s
    assertThat(ordered.getRelayedOn()).isNotNull();

    concurrent(2L, null);
    relay();
    assertThat(stamps.get(1)).isAfter(OffsetDateTime.now().plusSeconds(100)); // lease 2min

    when(publisher.lease(any())).thenReturn(Duration.ofSeconds(3));
    concurrent(3L, null);
    relay();
    assertThat(stamps.get(2)).isBefore(OffsetDateTime.now().plusSeconds(4));
  }

  // ------------------------------------------------------------ cancel against a live lease

  @Test
  void aRowCancelledInFlight_thatIsDelivered_isBookedSentButCancelled() {
    OutboxEvent event = pending(1L, 0);
    List<Long> listened = new ArrayList<>();
    listeners.add(e -> listened.add(e.getId()));
    doAnswer(
            inv -> {
              event.setCancelledOn(OffsetDateTime.now()); // ops cancel, the lease still live
              return null;
            })
        .when(publisher)
        .deliver(event);

    relay();

    assertThat(event.getRelayedOn()).isNotNull();
    assertThat(event.getCancelledOn()).isNotNull(); // both stamps - the truth on /ops
    assertThat(listened).containsExactly(1L); // the effect DID leave: its bookkeeping runs
    assertThat(bookings).extracting(OutboxBooking::outcome).containsExactly(Outcome.RELAYED);
  }

  @Test
  void aRowCancelledInFlight_thatFails_retiresCancelled_noRetryNoPark() {
    OutboxEvent event = pending(1L, properties.getMaxAttempts() - 1);
    doAnswer(
            inv -> {
              event.setCancelledOn(OffsetDateTime.now());
              throw new RuntimeException("receiver 503");
            })
        .when(publisher)
        .deliver(event);

    relay();

    assertThat(event.getParkedOn()).isNull();
    assertThat(event.getRelayedOn()).isNull();
    assertThat(event.getClaimedUntil()).isNull();
    assertThat(event.getLastError()).contains("receiver 503");
    assertThat(event.getNextAttemptOn()).isBeforeOrEqualTo(OffsetDateTime.now()); // no lease end
    assertThat(bookings).extracting(OutboxBooking::outcome).containsExactly(Outcome.CANCELLED);
  }

  // ------------------------------------------------------------ ORDERED renewal

  @Test
  void eachOrderedLease_isRenewedRightBeforeItsSend_byThePreviousBooking() {
    OutboxEvent first = pending(1L, 0);
    OutboxEvent second = pending(2L, 0);
    Map<Long, OffsetDateTime> leaseAtSend = new LinkedHashMap<>();
    doAnswer(
            inv -> {
              OutboxEvent row = inv.getArgument(0);
              leaseAtSend.put(row.getId(), table.get(row.getId()).getClaimedUntil());
              return null;
            })
        .when(publisher)
        .deliver(any());

    relay();

    assertThat(first.getRelayedOn()).isNotNull();
    assertThat(second.getRelayedOn()).isNotNull();
    assertThat(leaseAtSend.get(2L)).isAfter(leaseAtSend.get(1L)); // renewed, not the claim's
  }

  @Test
  void theRenewal_reachesExactlyOneRowAhead_andMovesItsNextAttemptToo() {
    pending(1L, 0);
    pending(2L, 0);
    pending(3L, 0);
    List<Boolean> nextAttemptIsLeaseEnd = new ArrayList<>();
    doAnswer(
            inv -> {
              OutboxEvent sent = table.get(inv.<OutboxEvent>getArgument(0).getId());
              nextAttemptIsLeaseEnd.add(sent.getNextAttemptOn().equals(sent.getClaimedUntil()));
              return null;
            })
        .when(publisher)
        .deliver(any());

    relay();

    // own guarded booking x3 + one renewal for row 2 and one for row 3 - never two rows ahead
    verify(repository, times(3)).bookRelayed(anyLong(), any(), any());
    verify(repository, times(2)).lockLeased(anyLong(), any());
    assertThat(nextAttemptIsLeaseEnd).containsExactly(true, true, true);
  }

  @Test
  void aRowLostToAnotherHolder_passesTheRenewalOnToTheRowAfterIt() {
    OutboxEvent first = pending(1L, 0);
    OutboxEvent taken = pending(2L, 0);
    pending(3L, 0);
    Map<Long, OffsetDateTime> leaseAtSend = new LinkedHashMap<>();
    Map<Long, OffsetDateTime> claimStamp = new LinkedHashMap<>();
    doAnswer(
            inv -> {
              OutboxEvent row = table.get(inv.<OutboxEvent>getArgument(0).getId());
              if (row.getId() == 1L) {
                claimStamp.put(3L, table.get(3L).getClaimedUntil());
                taken.setClaimedUntil(OffsetDateTime.now().plusHours(1)); // re-stamped elsewhere
              }
              leaseAtSend.put(row.getId(), row.getClaimedUntil());
              return null;
            })
        .when(publisher)
        .deliver(any());

    relay();

    assertThat(first.getRelayedOn()).isNotNull();
    assertThat(leaseAtSend).containsOnlyKeys(1L, 3L);
    assertThat(leaseAtSend.get(3L)).isAfter(claimStamp.get(3L)); // renewed, not the claim's
  }

  @Test
  void aRowCancelledBeforeItsTurn_passesTheRenewalOnToTheRowAfterIt() {
    pending(1L, 0);
    OutboxEvent cancelled = pending(2L, 0);
    pending(3L, 0);
    Map<Long, OffsetDateTime> claimStamp = new LinkedHashMap<>();
    Map<Long, OffsetDateTime> leaseAtSend = new LinkedHashMap<>();
    doAnswer(
            inv -> {
              OutboxEvent row = table.get(inv.<OutboxEvent>getArgument(0).getId());
              if (row.getId() == 1L) {
                claimStamp.put(3L, table.get(3L).getClaimedUntil());
                cancelled.setCancelledOn(OffsetDateTime.now());
              }
              leaseAtSend.put(row.getId(), row.getClaimedUntil());
              return null;
            })
        .when(publisher)
        .deliver(any());

    relay();

    assertThat(leaseAtSend).containsOnlyKeys(1L, 3L);
    assertThat(leaseAtSend.get(3L)).isAfter(claimStamp.get(3L));
  }

  @Test
  void anOrderedRowCancelledBeforeItsTurn_isReleasedUnsent_andTheNextIsRenewed() {
    OutboxEvent first = pending(1L, 0);
    OutboxEvent cancelled = pending(2L, 0);
    OutboxEvent third = pending(3L, 0);
    doAnswer(
            inv -> {
              cancelled.setCancelledOn(OffsetDateTime.now());
              return null;
            })
        .when(publisher)
        .deliver(first);

    relay();

    verify(publisher, never()).deliver(cancelled);
    assertThat(cancelled.getClaimedUntil()).isNull();
    assertThat(cancelled.getRelayedOn()).isNull();
    assertThat(third.getRelayedOn()).isNotNull();
  }

  /**
   * 1.9.0: an ORDERED row behind the batch head whose previous lease lapsed while its send was
   * under way - its turn books the lapse (no send, no renewal), and the batch goes on.
   */
  @Test
  void anOrderedRowWhoseSendLapsedBefore_isCountedInItsTurn_andTheBatchGoesOn() {
    OutboxEvent first = pending(1L, 0);
    OutboxEvent lapsed = lapsedWhileSending(2L, 0, Lane.ORDERED);
    OutboxEvent third = pending(3L, 0);

    relay();

    assertThat(first.getRelayedOn()).isNotNull();
    verify(publisher, never()).deliver(lapsed);
    assertThat(lapsed.getAttempts()).isEqualTo(1);
    assertThat(lapsed.getLastError()).startsWith("OutboxLeaseLapsedException");
    assertThat(third.getRelayedOn()).isNotNull();
  }

  @Test
  void anOrderedRowWhoseLeaseWasTaken_isSkipped() {
    OutboxEvent first = pending(1L, 0);
    OutboxEvent taken = pending(2L, 0);
    OffsetDateTime otherHolder = OffsetDateTime.now().plusHours(1);
    doAnswer(
            inv -> {
              taken.setClaimedUntil(otherHolder);
              return null;
            })
        .when(publisher)
        .deliver(first);

    relay();

    verify(publisher, never()).deliver(taken);
    assertThat(taken.getClaimedUntil()).isEqualTo(otherHolder);
  }

  @Test
  void anInFlightOrderedRow_isNotClaimedAgain() {
    OutboxEvent leased = pending(1L, 0);
    leasedElsewhere(leased);

    assertThat(relay()).isZero();
    verify(publisher, never()).deliver(leased);
  }

  @Test
  void theOrderedLane_takesAtMostOneBatch() {
    properties.setBatchSize(2);
    pending(1L, 0);
    pending(2L, 0);
    OutboxEvent third = pending(3L, 0);

    assertThat(relay()).isEqualTo(2);
    assertThat(third.getRelayedOn()).isNull();
    assertThat(relay()).isEqualTo(1);
    assertThat(third.getRelayedOn()).isNotNull();
  }

  @Test
  void aSystemicCommitFailure_inTheBooking_propagatesNothing_andStopsTheBatch() {
    OutboxEvent first = pending(1L, 0);
    OutboxEvent second = pending(2L, 0);
    doAnswer(
            inv -> {
              failCommits = true; // the database goes away while the first row is sent
              return null;
            })
        .when(publisher)
        .deliver(first);

    relay();

    verify(publisher, never()).deliver(second);
    assertThat(first.getRelayedOn()).isNull(); // nothing booked: redelivered after the lease
  }

  // ------------------------------------------------------------ the CONCURRENT lane

  @Test
  void concurrentRows_runOffTheRelayThread_andReturnTheirSlots() {
    OutboxEvent row = concurrent(1L, null);
    List<String> threads = new CopyOnWriteArrayList<>();
    doAnswer(
            inv -> {
              threads.add(Thread.currentThread().getName());
              return null;
            })
        .when(publisher)
        .deliver(any());

    assertThat(relay()).isEqualTo(1);

    await().atMost(Duration.ofSeconds(5)).until(() -> row.getRelayedOn() != null);
    assertThat(threads).singleElement().asString().startsWith("opentmf-outbox-send-");
    assertThat(lane.isVirtual()).isFalse();
  }

  @Test
  void theStampedLaneWins_overWhatThePublisherSaysNow() {
    when(publisher.lane(any())).thenReturn(Lane.CONCURRENT); // the publisher changed its mind
    OutboxEvent stampedOrdered = pending(1L, 0); // appended as ORDERED (or before 1.3.0)
    OutboxEvent stampedConcurrent = concurrent(2L, null);
    when(publisher.lane(stampedConcurrent)).thenReturn(Lane.ORDERED);
    List<String> threads = new CopyOnWriteArrayList<>();
    doAnswer(
            inv -> {
              long id = inv.<OutboxEvent>getArgument(0).getId();
              threads.add(id + "@" + Thread.currentThread().getName());
              return null;
            })
        .when(publisher)
        .deliver(any());

    relay();

    await().atMost(Duration.ofSeconds(5)).until(() -> stampedConcurrent.getRelayedOn() != null);
    assertThat(stampedOrdered.getRelayedOn()).isNotNull();
    assertThat(threads)
        .anyMatch(t -> t.equals("1@opentmf-outbox-ordered")) // the ORDERED lane's thread
        .anyMatch(t -> t.startsWith("2@opentmf-outbox-send-"));
  }

  @Test
  void theClaim_movesNextAttemptToTheLeaseEnd_andTheBookingMovesItBack() {
    OutboxEvent row = pending(1L, 0);
    List<OffsetDateTime> atSend = new ArrayList<>();
    doAnswer(
            inv -> {
              OutboxEvent sent = table.get(1L);
              assertThat(sent.getNextAttemptOn()).isEqualTo(sent.getClaimedUntil());
              atSend.add(sent.getNextAttemptOn());
              return null;
            })
        .when(publisher)
        .deliver(any());

    relay();

    assertThat(atSend).singleElement().satisfies(t -> assertThat(t).isAfter(OffsetDateTime.now()));
    assertThat(row.getNextAttemptOn()).isEqualTo(row.getRelayedOn()); // no future value left
  }

  @Test
  void theClaim_takesAsManyRowsAsTheLaneHasRoom_aheadOfTheSlots_andTheSlotsBoundTheSends() {
    OutboxConcurrentLane roomy = fast(new OutboxConcurrentLane(2, 4, false, Duration.ofSeconds(5)));
    OutboxRelayWorker ahead = workerOn(roomy);
    for (long id = 1; id <= 6; id++) {
      concurrent(id, null);
    }
    CountDownLatch gate = new CountDownLatch(1);
    List<Long> sending = new CopyOnWriteArrayList<>();
    doAnswer(
            inv -> {
              sending.add(inv.<OutboxEvent>getArgument(0).getId());
              gate.await();
              return null;
            })
        .when(publisher)
        .deliver(any());
    try {
      OutboxRelayWorker.Batch batch = ahead.relayBatch(true);

      assertThat(batch.concurrent()).isEqualTo(4); // the room: the queue's capacity
      assertThat(batch.concurrentFull()).isTrue(); // more may wait - the relay claims again
      await().atMost(Duration.ofSeconds(5)).until(() -> sending.size() == 2);
      assertThat(roomy.queued()).isEqualTo(2); // leased, waiting for a slot
      assertThat(table.get(4L).getClaimedUntil()).isNotNull();
      assertThat(table.get(5L).getClaimedUntil()).isNull(); // beyond the room: not claimed
      assertThat(ahead.relayBatch(true).concurrent()).isEqualTo(2); // half: up to the capacity
      assertThat(roomy.queued()).isEqualTo(4);
      assertThat(ahead.relayBatch(true).concurrent()).isZero(); // above half: no claim
      assertThat(sending).hasSize(2); // the slots, not the claim, bound the sends
      gate.countDown();
      idle(roomy, 2);
      assertThat(table.values()).allMatch(row -> row.getRelayedOn() != null);
    } finally {
      gate.countDown();
      roomy.close();
    }
  }

  @Test
  void aQueueAboveHalf_meansNoConcurrentClaimAtAll() {
    OutboxEvent row = concurrent(1L, null);
    CountDownLatch gate = new CountDownLatch(1);
    for (int i = 0; i < 4; i++) { // two slots busy, two waiting: above half of two
      lane.submit(
          () -> {
            try {
              gate.await();
            } catch (InterruptedException ex) {
              Thread.currentThread().interrupt();
            }
          });
    }

    assertThat(worker.relayBatch(true).claimed()).isZero();

    verify(repository, never()).claimConcurrent(any(), any(), anyInt());
    assertThat(row.getClaimedUntil()).isNull();
    gate.countDown();
    idle(lane, 2);
  }

  @Test
  void withoutTheOrderedWord_theOrderedLaneIsNotClaimed() {
    OutboxEvent ordered = pending(1L, 0);
    OutboxEvent conc = concurrent(2L, null);

    OutboxRelayWorker.Batch batch = worker.relayBatch(false);
    idle(lane, 2);

    assertThat(batch.ordered()).isZero();
    assertThat(batch.concurrent()).isEqualTo(1);
    verify(repository, never()).claimOrdered(any(), anyInt());
    assertThat(ordered.getClaimedUntil()).isNull();
    assertThat(conc.getRelayedOn()).isNotNull();
  }

  @Test
  void aFullOrderedClaim_saysSo() {
    properties.setBatchSize(2);
    pending(1L, 0);
    pending(2L, 0);
    pending(3L, 0);

    OutboxRelayWorker.Batch full = worker.relayBatch(true);
    orderedIdle();
    OutboxRelayWorker.Batch rest = worker.relayBatch(true);
    orderedIdle();

    assertThat(full.ordered()).isEqualTo(2);
    assertThat(full.orderedFull()).isTrue();
    assertThat(full.concurrentFull()).isFalse(); // the CONCURRENT claim found nothing
    assertThat(rest.ordered()).isEqualTo(1);
    assertThat(rest.orderedFull()).isFalse();
  }

  /**
   * OUTBOX-CONCURRENT-CLAIM-INDEPENDENT-1 (1.8.0): the ORDERED batch is sent on the ORDERED lane's
   * thread, so the claim returns while it sends - and while it sends, the next batch claims the
   * CONCURRENT lane and leaves the ORDERED lane alone (one ORDERED batch in flight per pod).
   */
  @Test
  void theOrderedBatch_isSentOffTheClaimingThread_andNotClaimedAgainWhileItSends() {
    OutboxEvent first = pending(1L, 0);
    pending(2L, 0);
    CountDownLatch gate = new CountDownLatch(1);
    List<String> senders = new CopyOnWriteArrayList<>();
    doAnswer(
            inv -> {
              senders.add(Thread.currentThread().getName());
              if (inv.<OutboxEvent>getArgument(0).getId() == 1L) {
                gate.await();
              }
              return null;
            })
        .when(publisher)
        .deliver(any());

    OutboxRelayWorker.Batch batch = worker.relayBatch(true); // returns while row 1 is sending

    assertThat(batch.ordered()).isEqualTo(2);
    await().atMost(Duration.ofSeconds(5)).until(() -> senders.size() == 1);
    assertThat(orderedLane.busy()).isTrue();
    OutboxEvent conc = concurrent(3L, null);
    OutboxRelayWorker.Batch next = worker.relayBatch(true); // the ORDERED lane is still sending
    idle(lane, 2);
    assertThat(next.ordered()).isZero();
    assertThat(next.concurrent()).isEqualTo(1);
    assertThat(conc.getRelayedOn()).isNotNull(); // sent while the ORDERED batch waits
    assertThat(first.getRelayedOn()).isNull();
    verify(repository, times(1)).claimOrdered(any(), anyInt());

    gate.countDown();
    orderedIdle();
    assertThat(table.get(1L).getRelayedOn()).isNotNull();
    assertThat(table.get(2L).getRelayedOn()).isNotNull();
    assertThat(senders) // the ORDERED lane's thread, not the caller's
        .doesNotContain(Thread.currentThread().getName())
        .first()
        .isEqualTo("opentmf-outbox-ordered");
  }

  @Test
  void oneStatementStampsEveryLeaseOfOneLength_neverOneUpdatePerRow() {
    OutboxConcurrentLane roomy =
        fast(new OutboxConcurrentLane(2, 10, false, Duration.ofSeconds(5)));
    OutboxRelayWorker ahead = workerOn(roomy);
    concurrent(1L, null);
    concurrent(2L, null);
    OutboxEvent ownLease = concurrent(3L, null);
    when(publisher.lease(ownLease)).thenReturn(Duration.ofSeconds(30));
    try {
      ahead.relayBatch(true);
      idle(roomy, 2);
    } finally {
      roomy.close();
    }

    verify(repository).stampLeases(any(), eq("{1,2}")); // the library lease
    verify(repository).stampLeases(any(), eq("{3}")); // the publisher's own
    verify(repository, times(2)).stampLeases(any(), any());
  }

  /**
   * The claim writes the leases by SQL, and the rows it returns are not the table's own (JPA's
   * entities detach when the claim commits): the stamp must reach the table AND be mirrored on the
   * row handed to the publisher - on both lanes.
   */
  @Test
  void theRowHandedToThePublisher_carriesTheLeaseTheClaimWrote_onBothLanes() {
    OutboxEvent ordered = pending(1L, 0);
    OutboxEvent conc = concurrent(2L, null);
    doAnswer(inv -> List.of(copy(table.get(1L)))) // detached, as after the claim's commit
        .when(repository)
        .claimOrdered(any(), anyInt());
    doAnswer(inv -> List.of(copy(table.get(2L))))
        .when(repository)
        .claimConcurrent(any(), any(), anyInt());
    List<OutboxEvent> atSend = new CopyOnWriteArrayList<>();
    doAnswer(
            inv -> {
              atSend.add(copy(inv.getArgument(0)));
              return null;
            })
        .when(publisher)
        .deliver(any());

    relay();

    assertThat(atSend)
        .hasSize(2)
        .allSatisfy(
            sent -> {
              assertThat(sent.getClaimedUntil()).isNotNull();
              assertThat(sent.getNextAttemptOn()).isEqualTo(sent.getClaimedUntil());
            });
    // the table carried the leases, so each guarded booking matched
    assertThat(ordered.getRelayedOn()).isNotNull();
    assertThat(conc.getRelayedOn()).isNotNull();
  }

  @Test
  void aRelayedRow_isBookedByOneStatement_andNeverLockedFirst() {
    OutboxEvent row = concurrent(1L, null);

    relay();

    assertThat(row.getRelayedOn()).isNotNull();
    verify(repository).bookRelayed(eq(1L), any(), any());
    verify(repository, never()).lockLeased(anyLong(), any()); // no renewal, no failure
  }

  /**
   * 1.12.0: once a stopping relay's grace has run out the worker books NOTHING - a send that
   * returns afterwards leaves its row to its lease, redelivered once it lapses - so no booking
   * borrows a connection while the application's pool closes; the rest of an ORDERED batch is not
   * sent at all.
   */
  @Test
  void aSendThatReturnsAfterTheGrace_booksNothing_andTheBatchStops() {
    OutboxEvent returningLate = pending(1L, 0);
    OutboxEvent next = pending(2L, 0);
    doAnswer(
            inv -> {
              worker.close(); // the grace runs out while this send is under way
              return null;
            })
        .when(publisher)
        .deliver(returningLate);

    relay();

    assertThat(returningLate.getRelayedOn()).as("booked by nobody").isNull();
    assertThat(returningLate.getAttempts()).isZero();
    verify(publisher, never()).deliver(next); // the batch's rest is not sent
    verify(repository, never()).bookRelayed(anyLong(), any(), any());
  }

  // ------------------------------------------------------------ a queued row's lease

  /** A one-slot lane whose first send (row 1) holds the slot until the gate opens. */
  private OutboxConcurrentLane oneSlotHeldBy(CountDownLatch gate, List<Long> delivered) {
    doAnswer(
            inv -> {
              long id = inv.<OutboxEvent>getArgument(0).getId();
              delivered.add(id);
              if (id == 1L) {
                gate.await();
              }
              return null;
            })
        .when(publisher)
        .deliver(any());
    return fast(new OutboxConcurrentLane(1, 10, false, Duration.ofSeconds(5)));
  }

  /** Longer than a tenth of a one-second lease. */
  private static void waitPastATenth() throws InterruptedException {
    assertThat(new CountDownLatch(1).await(150, TimeUnit.MILLISECONDS)).isFalse();
  }

  @Test
  void aQueuedRowThatWaitedATenthOfItsLease_isRenewedBeforeItsSend() throws Exception {
    when(publisher.lease(any())).thenReturn(Duration.ofSeconds(1));
    CountDownLatch gate = new CountDownLatch(1);
    List<Long> delivered = new CopyOnWriteArrayList<>();
    OutboxConcurrentLane oneSlot = oneSlotHeldBy(gate, delivered);
    OutboxRelayWorker ahead = workerOn(oneSlot);
    concurrent(1L, null);
    OutboxEvent queued = concurrent(2L, null);
    List<OffsetDateTime> atSend = new CopyOnWriteArrayList<>();
    try {
      ahead.relayBatch(true);
      OffsetDateTime claimed = queued.getClaimedUntil();
      waitPastATenth();
      doAnswer(
              inv -> {
                atSend.add(table.get(2L).getClaimedUntil());
                return null;
              })
          .when(publisher)
          .deliver(queued);
      gate.countDown();
      idle(oneSlot, 1);

      assertThat(atSend).singleElement().satisfies(t -> assertThat(t).isAfter(claimed));
      assertThat(queued.getRelayedOn()).isNotNull();
      verify(repository).lockLeased(2L, claimed); // the guarded renewal
      verify(repository, never()).lockLeased(eq(1L), any()); // handed straight to the slot
    } finally {
      gate.countDown();
      oneSlot.close();
    }
  }

  /**
   * OUTBOX-QUEUED-LEASE-STARVATION-1 (1.9.0): a row this pod still holds queued is never claimed
   * again - not even once its lease lapsed in the queue. It keeps its place; when the slot frees it
   * is renewed under its own, untaken lease and sent once. Up to 1.8.0 the next pass claimed it
   * again and queued it at the tail, and its old place was dropped as lost.
   */
  @Test
  void aQueuedRowWhoseLeaseLapsed_isNeverClaimedAgain_andIsSentOnceInItsPlace() {
    when(publisher.lease(any())) // row 1's send outlasts the test; row 2 has a one-second lease
        .thenAnswer(
            inv ->
                inv.<OutboxEvent>getArgument(0).getId() == 1L
                    ? Duration.ofMinutes(2)
                    : Duration.ofSeconds(1));
    CountDownLatch gate = new CountDownLatch(1);
    List<Long> delivered = new CopyOnWriteArrayList<>();
    OutboxConcurrentLane oneSlot = oneSlotHeldBy(gate, delivered);
    OutboxRelayWorker ahead = workerOn(oneSlot);
    concurrent(1L, null);
    concurrent(2L, null);
    try {
      ahead.relayBatch(true); // row 1 holds the slot, row 2 waits in the queue
      OffsetDateTime claimed = table.get(2L).getClaimedUntil();
      await() // its one-second lease runs out while it waits
          .atMost(Duration.ofSeconds(5))
          .until(() -> OffsetDateTime.now(ZoneOffset.UTC).isAfter(claimed.plusNanos(100_000_000)));

      OutboxRelayWorker.Batch again = ahead.relayBatch(true);

      assertThat(again.concurrent()).isZero(); // not claimed a second time
      assertThat(table.get(2L).getClaimedUntil()).isEqualTo(claimed); // its own lease, untouched
      assertThat(oneSlot.queued()).isEqualTo(1); // one place in the queue, not two
      gate.countDown();
      idle(oneSlot, 1);
      assertThat(delivered).containsExactly(1L, 2L); // sent once, in its place
      verify(repository).lockLeased(2L, claimed); // renewed under its own lease first
      assertThat(table.get(2L).getRelayedOn()).isNotNull();
    } finally {
      gate.countDown();
      oneSlot.close();
    }
  }

  /**
   * 1.9.0 (K1): once no slot is live - the one slot busy with a send older than a second - the
   * row waiting behind it is given back: its lease released in one guarded statement, due at
   * once, the row out of the queue unsent; this pod may claim it again later, any pod now.
   */
  @Test
  void aQueueBehindSlotsThatTurnedSlow_isGivenBack_unsent() {
    CountDownLatch gate = new CountDownLatch(1);
    List<Long> delivered = new CopyOnWriteArrayList<>();
    OutboxConcurrentLane oneSlot = oneSlotHeldBy(gate, delivered);
    OutboxRelayWorker ahead = workerOn(oneSlot);
    concurrent(1L, null);
    concurrent(2L, null);
    when(repository.releaseLeases(any(), any(), any()))
        .thenAnswer(
            inv -> {
              List<Long> ids = idsOf(inv.getArgument(0));
              ids.forEach(
                  id -> {
                    table.get(id).setClaimedUntil(null);
                    table.get(id).setNextAttemptOn(inv.getArgument(2));
                  });
              return ids.size();
            });
    try {
      ahead.relayBatch(true); // row 1 holds the slot, row 2 waits in the queue
      OffsetDateTime stamp = table.get(2L).getClaimedUntil();
      ahead.giveBackStranded(); // the slot's send is young: nothing given back
      verify(repository, never()).releaseLeases(any(), any(), any());

      await()
          .pollDelay(OutboxConcurrentLane.HORIZON.plusMillis(100))
          .atMost(Duration.ofSeconds(5))
          .until(() -> oneSlot.liveSlots() == 0);
      ahead.giveBackStranded();

      ArgumentCaptor<String> stamps = ArgumentCaptor.forClass(String.class);
      verify(repository).releaseLeases(eq("{2}"), stamps.capture(), any());
      assertThat(stamps.getValue()).startsWith("{\"" + stamp.toLocalDate());
      assertThat(table.get(2L).getClaimedUntil()).isNull(); // due at once, for any pod
      assertThat(oneSlot.queued()).isZero();
      gate.countDown();
      idle(oneSlot, 1);
      assertThat(delivered).containsExactly(1L); // row 2 was not sent from the queue
      assertThat(ahead.relayBatch(true).concurrent()).isEqualTo(1); // claimable again, here too
    } finally {
      gate.countDown();
      oneSlot.close();
    }
  }

  @Test
  void aQueuedRowThatWaitedBriefly_isSentOnItsClaimLease() {
    CountDownLatch gate = new CountDownLatch(1);
    List<Long> delivered = new CopyOnWriteArrayList<>();
    OutboxConcurrentLane oneSlot = oneSlotHeldBy(gate, delivered); // the 2 min library lease
    OutboxRelayWorker ahead = workerOn(oneSlot);
    concurrent(1L, null);
    OutboxEvent queued = concurrent(2L, null);
    try {
      ahead.relayBatch(true);
      gate.countDown();
      idle(oneSlot, 1);

      assertThat(queued.getRelayedOn()).isNotNull();
      verify(repository, never()).lockLeased(anyLong(), any());
    } finally {
      oneSlot.close();
    }
  }

  @Test
  void aQueuedRowTakenByAnotherHolderMeanwhile_isNotSent() throws Exception {
    when(publisher.lease(any())).thenReturn(Duration.ofSeconds(1));
    CountDownLatch gate = new CountDownLatch(1);
    List<Long> delivered = new CopyOnWriteArrayList<>();
    OutboxConcurrentLane oneSlot = oneSlotHeldBy(gate, delivered);
    OutboxRelayWorker ahead = workerOn(oneSlot);
    concurrent(1L, null);
    OutboxEvent queued = concurrent(2L, null);
    try {
      ahead.relayBatch(true);
      waitPastATenth();
      leasedElsewhere(queued); // its lease lapsed, another relay took it
      gate.countDown();
      idle(oneSlot, 1);

      assertThat(delivered).containsExactly(1L);
      assertThat(queued.getRelayedOn()).isNull();
    } finally {
      gate.countDown();
      oneSlot.close();
    }
  }

  @Test
  void aQueuedRowCancelledMeanwhile_isReleasedUnsent() throws Exception {
    when(publisher.lease(any())).thenReturn(Duration.ofSeconds(1));
    CountDownLatch gate = new CountDownLatch(1);
    List<Long> delivered = new CopyOnWriteArrayList<>();
    OutboxConcurrentLane oneSlot = oneSlotHeldBy(gate, delivered);
    OutboxRelayWorker ahead = workerOn(oneSlot);
    concurrent(1L, null);
    OutboxEvent queued = concurrent(2L, null);
    try {
      ahead.relayBatch(true);
      waitPastATenth();
      queued.setCancelledOn(OffsetDateTime.now());
      gate.countDown();
      idle(oneSlot, 1);

      assertThat(delivered).containsExactly(1L);
      assertThat(queued.getClaimedUntil()).isNull(); // released
      assertThat(queued.getRelayedOn()).isNull();
    } finally {
      gate.countDown();
      oneSlot.close();
    }
  }

  @Test
  void aRenewalThatFails_isLogged_theRowIsNotSent_andTheLaneGoesOn() throws Exception {
    when(publisher.lease(any())).thenReturn(Duration.ofSeconds(1));
    CountDownLatch gate = new CountDownLatch(1);
    List<Long> delivered = new CopyOnWriteArrayList<>();
    OutboxConcurrentLane oneSlot = oneSlotHeldBy(gate, delivered);
    OutboxRelayWorker ahead = workerOn(oneSlot);
    concurrent(1L, null);
    concurrent(2L, null);
    OutboxEvent third = concurrent(3L, null);
    try {
      ahead.relayBatch(true);
      waitPastATenth();
      when(repository.lockLeased(eq(2L), any()))
          .thenThrow(new IllegalStateException("database gone mid-renewal"));
      gate.countDown();
      idle(oneSlot, 1);

      assertThat(delivered).containsExactly(1L, 3L);
      assertThat(third.getRelayedOn()).isNotNull();
    } finally {
      gate.countDown();
      oneSlot.close();
    }
  }

  @Test
  void theKeysLiteral_keepsEveryKeyOneElement() {
    assertThat(
            OutboxRelayWorker.keysLiteral(
                List.of("plain", "with,comma", "with \"quote\"", "back\\slash", "{brace}")))
        .isEqualTo(
            "{\"plain\",\"with,comma\",\"with \\\"quote\\\"\",\"back\\\\slash\",\"{brace}\"}");
  }

  @Test
  void rowsSharingAnOrderingKey_areNeverInFlightTogether_andGoInIdOrder() {
    OutboxEvent firstOfA = concurrent(1L, "receiver-a");
    OutboxEvent secondOfA = concurrent(2L, "receiver-a");
    OutboxEvent independent = concurrent(3L, null);

    assertThat(relay()).isEqualTo(2); // 1 and 3 - the second of the key waits its turn
    assertThat(firstOfA.getRelayedOn()).isNotNull();
    assertThat(independent.getRelayedOn()).isNotNull();
    assertThat(secondOfA.getRelayedOn()).isNull();

    assertThat(relay()).isEqualTo(1);
    assertThat(secondOfA.getRelayedOn()).isNotNull();
  }

  @Test
  void aRowCancelledWhileInFlight_stillHoldsItsKey() {
    OutboxEvent flying = concurrent(1L, "receiver-a");
    leasedElsewhere(flying);
    flying.setCancelledOn(OffsetDateTime.now()); // cancelled, but its send is still running
    OutboxEvent next = concurrent(2L, "receiver-a");

    assertThat(relay()).isZero();
    assertThat(next.getClaimedUntil()).isNull();
  }

  @Test
  void aConcurrentClaimThatDoesNotCommit_isLogged_andTheOrderedRowsGoOn() {
    concurrent(1L, null);
    OutboxEvent ordered = pending(2L, 0);
    when(repository.claimConcurrent(any(), any(), anyInt()))
        .thenThrow(new IllegalStateException("database gone mid-claim"));

    worker.relayBatch(true).claimed(); // logged - the pass goes on
    orderedIdle();

    assertThat(lane.available()).isEqualTo(2);
    assertThat(ordered.getRelayedOn()).isNotNull();
  }

  @Test
  void aKeyAnotherPodIsClaiming_isSkippedThisPass_unkeyedRowsGoOn() {
    OutboxEvent keyed = concurrent(1L, "receiver-a");
    OutboxEvent unkeyed = concurrent(2L, null);
    doReturn(List.of()).when(repository).tryLockKeys(anyInt(), any()); // another pod has it

    assertThat(relay()).isEqualTo(1);

    assertThat(keyed.getClaimedUntil()).isNull(); // not stamped: left for the next pass
    assertThat(unkeyed.getRelayedOn()).isNotNull();
    verify(repository, never()).recheckKeyedHeads(any(), any()); // nothing keyed to re-check
  }

  @Test
  void aLockedKeysCandidate_thatTheReCheckDrops_isNotClaimed() {
    OutboxEvent keyed = concurrent(1L, "receiver-a");
    doReturn(List.of()).when(repository).recheckKeyedHeads(any(), any()); // in flight by now

    assertThat(relay()).isZero();

    assertThat(keyed.getClaimedUntil()).isNull();
    verify(repository).recheckKeyedHeads(any(), eq("{1}"));
  }

  @Test
  void everyKeyIsTriedInOneStatement_andTheReCheckSeesEveryLockedCandidate() {
    concurrent(1L, "receiver-a");
    concurrent(2L, "receiver-b");
    concurrent(3L, null);
    concurrent(4L, "receiver-c");
    doAnswer(inv -> List.of(table.get(1L), table.get(2L), table.get(3L), table.get(4L)))
        .when(repository)
        .claimConcurrent(any(), any(), anyInt());
    doReturn(List.of("receiver-a", "receiver-b")) // receiver-c: another pod is claiming it
        .when(repository)
        .tryLockKeys(anyInt(), any());

    relay();

    // one statement for every distinct key; an unkeyed row needs no lock
    verify(repository)
        .tryLockKeys(
            OutboxClaimSql.KEY_LOCK_NAMESPACE, "{\"receiver-a\",\"receiver-b\",\"receiver-c\"}");
    verify(repository, times(1)).tryLockKeys(anyInt(), any());
    verify(repository).recheckKeyedHeads(any(), eq("{1,2}"));
    assertThat(table.get(4L).getClaimedUntil()).isNull();
  }

  @Test
  void unkeyedCandidatesAlone_takeNoLockAndNoReCheck() {
    OutboxEvent row = concurrent(1L, null);

    relay();

    assertThat(row.getRelayedOn()).isNotNull();
    verify(repository, never()).tryLockKeys(anyInt(), any());
    verify(repository, never()).recheckKeyedHeads(any(), any());
  }

  @Test
  void aConcurrentClaimWhoseCommitFails_leavesNothingStamped_andSendsNone() {
    OutboxEvent row = concurrent(1L, null);
    doAnswer(
            inv -> {
              failCommits = true; // the stamps are set, then the commit fails
              return claimConcurrent(
                  inv.getArgument(0), inv.getArgument(1), null, inv.getArgument(2), true);
            })
        .when(repository)
        .claimConcurrent(any(), any(), anyInt());

    assertThat(worker.relayBatch(true).claimed()).isZero(); // nothing claimed, nothing submitted

    assertThat(lane.available()).isEqualTo(2);
    verify(publisher, never()).deliver(row);
    assertThat(row.getClaimedUntil()).isNull(); // rolled back
  }

  @Test
  void anOrderedClaimThatDoesNotCommit_failsThePass() {
    pending(1L, 0);
    failCommits = true;

    assertThatThrownBy(() -> worker.relayBatch(true)).hasMessageContaining("database gone");

    assertThat(lane.available()).isEqualTo(2);
  }

  @Test
  void keysAreServedRoundRobin_fromTheCursor_wrappingRound() {
    concurrent(1L, "a");
    concurrent(2L, "a");
    concurrent(3L, "b");
    concurrent(4L, "b");
    concurrent(5L, "c");
    concurrent(6L, "c");
    List<String> served = new CopyOnWriteArrayList<>();
    doAnswer(
            inv -> {
              served.add(inv.<OutboxEvent>getArgument(0).getOrderingKey());
              return null;
            })
        .when(publisher)
        .deliver(any());

    relay(); // two rows: a, b - the cursor moves to b
    List<String> first = List.copyOf(served);
    relay(); // c, then wraps round to a
    List<String> second = List.copyOf(served.subList(2, served.size()));
    relay(); // b, c

    // within a batch the two slots send in parallel; across batches every key has its turn
    assertThat(first).containsExactlyInAnyOrder("a", "b");
    assertThat(second).containsExactlyInAnyOrder("c", "a");
    assertThat(served.subList(4, served.size())).containsExactlyInAnyOrder("b", "c");
    verify(repository).claimConcurrent(any(), eq("b"), eq(2));
    verify(repository).claimConcurrentWrap(any(), eq("b"), eq(1)); // the wrap
    // after the wrap the cursor is the WRAPPED pass's last key (a), not the first pass's (c)
    verify(repository).claimConcurrent(any(), eq("a"), eq(2));
    // a pass that filled every slot never wraps
    verify(repository, times(1)).claimConcurrentWrap(any(), any(), anyInt());
  }

  /**
   * OUTBOX-D3-FAIRNESS-INVESTIGATION-1: a first pass that comes back short because another pod
   * holds some of its heads locked ({@code SKIP LOCKED}) has NOT run out of keys. It must not wrap:
   * a wrap would move the cursor back to the first keys, and every key above the short pass - due
   * all along - would lose its turn for a whole cycle (on a slow host, a contiguous block of rows
   * waiting minutes). The cursor moves on from the short pass's last key instead.
   */
  @Test
  void aFirstPassShortOnLockedHeads_doesNotWrap_theCursorMovesOnFromIt() {
    concurrent(1L, "a");
    concurrent(2L, "b");
    OutboxEvent lockedC = concurrent(3L, "c");
    concurrent(4L, "d");
    concurrent(5L, "e");
    concurrent(6L, "f");
    concurrent(7L, "a");
    concurrent(8L, "b");

    relay(); // a, b - the cursor moves to b
    lockedElsewhere.add(lockedC.getId()); // another pod is claiming c right now
    relay(); // heads c, d: c is skipped locked, d is claimed - short, but e and f are due above

    verify(repository).claimConcurrent(any(), eq("b"), eq(2));
    verify(repository, never()).claimConcurrentWrap(any(), any(), anyInt());
    lockedElsewhere.clear();
    relay(); // the cursor moved on from d: e, f - a's and b's second rows wait their turn
    verify(repository).claimConcurrent(any(), eq("d"), eq(2));
    assertThat(table.get(5L).getRelayedOn()).isNotNull();
    assertThat(table.get(6L).getRelayedOn()).isNotNull();
    assertThat(table.get(7L).getRelayedOn()).isNull();
    assertThat(table.get(8L).getRelayedOn()).isNull();
  }

  /**
   * A pass whose every head is held locked takes nothing - and still moves the cursor past them:
   * a head locked for longer (a consumer's transaction on the row) must never stop the lane.
   */
  @Test
  void aFirstPassWhoseEveryHeadIsLocked_takesNone_andTheCursorStillMovesPastThem() {
    concurrent(1L, "a");
    concurrent(2L, "b");
    concurrent(3L, "c");
    concurrent(4L, "d");
    concurrent(5L, "e");
    concurrent(6L, "f");

    relay(); // a, b - the cursor moves to b
    lockedElsewhere.addAll(List.of(3L, 4L)); // c and d, held locked for as long as this test runs
    assertThat(relay()).isZero(); // heads c, d - both locked: nothing taken, no wrap
    relay(); // the cursor moved past d: e, f

    verify(repository).claimConcurrent(any(), eq("d"), eq(2));
    verify(repository, never()).claimConcurrentWrap(any(), any(), anyInt());
    assertThat(table.get(5L).getRelayedOn()).isNotNull();
    assertThat(table.get(6L).getRelayedOn()).isNotNull();
    assertThat(table.get(3L).getRelayedOn()).isNull();
  }

  @Test
  void anUnroutableConcurrentRow_isBookedInTheClaim_andTakesNoSlot() {
    when(publisher.supports(any())).thenReturn(false);
    OutboxEvent row = concurrent(1L, null);

    assertThat(relay()).isZero();

    assertThat(row.getAttempts()).isEqualTo(1);
    assertThat(lane.available()).isEqualTo(2);
  }

  @Test
  void aSendTheClosingLaneRefuses_returnsItsSlot_theLeaseLapses() {
    OutboxEvent row = concurrent(1L, null);
    lane.close();

    assertThat(worker.relayBatch(true).claimed()).isEqualTo(1);

    assertThat(lane.available()).isEqualTo(2);
    assertThat(row.getRelayedOn()).isNull();
    assertThat(row.getClaimedUntil()).isNotNull();
  }

  @Test
  void aBookingFailureOnTheConcurrentLane_isLogged_andTheSlotStillReturns() {
    OutboxEvent row = concurrent(1L, null);
    doAnswer(
            inv -> {
              failCommits = true;
              return null;
            })
        .when(publisher)
        .deliver(row);

    worker.relayBatch(true).claimed();

    await().atMost(Duration.ofSeconds(5)).until(() -> lane.available() == 2);
    assertThat(row.getRelayedOn()).isNull();
  }

  @Test
  void anOrderedRow_isClaimedWhateverTheConcurrentBacklog() {
    CountDownLatch gate = new CountDownLatch(1);
    doAnswer(
            inv -> {
              if (inv.<OutboxEvent>getArgument(0).getLane() == Lane.CONCURRENT) {
                gate.await();
              }
              return null;
            })
        .when(publisher)
        .deliver(any());
    for (long id = 1; id <= 500; id++) {
      concurrent(id, "one-slow-receiver");
    }
    OutboxEvent ordered = pending(501L, 0);

    worker.relayBatch(true).claimed();
    orderedIdle(); // the ORDERED batch went out while the CONCURRENT slots stay blocked

    assertThat(ordered.getRelayedOn()).isNotNull();
    gate.countDown();
    await().atMost(Duration.ofSeconds(5)).until(() -> lane.available() == 2);
  }


  // ------------------------------------------------- the claim pause (1.11.0)

  /** Rows to "kafka:" destinations go over the transport "kafka"; every other row names none. */
  private void transportByDestination() {
    doAnswer(
            inv -> {
              OutboxEvent row = inv.getArgument(0);
              return row.getDestination().startsWith("kafka:") ? "kafka" : null;
            })
        .when(publisher)
        .transport(any());
  }

  private OutboxEvent overKafka(long id) {
    OutboxEvent row = pending(id, 0);
    row.setDestination("kafka:topic");
    return row;
  }

  private double claimsPaused(String transport) {
    return registry
        .get(OutboxMetrics.CLAIMS_PAUSED)
        .tag(OutboxMetrics.TAG_TRANSPORT, transport)
        .gauge()
        .value();
  }

  /**
   * OUTBOX-FAILING-RELAY-CLAIMS-1: five failed sends over one transport in a row pause it on this
   * pod. Its next rows are left - not leased, not attempted, no backoff - while a row of another
   * publisher behind them is taken in the same pass; once a {@code backoff-base} has passed ONE
   * probe row goes out, and its success resumes the transport.
   */
  @Test
  void fiveTransportFailuresInARow_leaveItsRowsToOtherPods_aProbeRowsSuccessResumesIt() {
    properties.setBackoffBase(Duration.ofMillis(300));
    transportByDestination();
    for (long id = 1; id <= 5; id++) {
      overKafka(id);
    }
    doThrow(new RuntimeException("broker unreachable")).when(publisher).deliver(any());

    relay(); // five sends over kafka, five failures: paused
    assertThat(claimsPaused("kafka")).isEqualTo(1d);
    OutboxEvent left = overKafka(6L);
    OutboxEvent behind = pending(7L, 0); // another publisher's row, behind the kafka rows
    doReturn(null).when(publisher).deliver(behind);
    OutboxRelayWorker.Batch skipping = worker.relayBatch(true);
    orderedIdle();

    assertThat(left.getAttempts()).isZero();
    assertThat(left.getClaimedUntil()).isNull(); // not leased: due for any other pod
    assertThat(behind.getRelayedOn()).isNotNull(); // the skip hid nothing behind it
    assertThat(skipping.orderedFull()).isFalse(); // no claim again at once over skipped rows
    verify(repository, atLeastOnce())
        .claimOrdered(any(), eq(properties.getBatchSize() * OutboxRelayWorker.SKIP_WINDOW));

    doReturn(null).when(publisher).deliver(any()); // the broker is back
    await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> {
              relay();
              return claimsPaused("kafka") == 0d;
            });
    relay();
    assertThat(left.getRelayedOn()).isNotNull();
    assertThat(table.values()).allSatisfy(row -> assertThat(row.getRelayedOn()).isNotNull());
  }

  /** A publisher naming no transport (the HTTP one) never pauses: a dead receiver is everyone's. */
  @Test
  void failuresOfAPublisherWithNoTransport_neverPause() {
    for (long id = 1; id <= 6; id++) {
      pending(id, 0);
    }
    doThrow(new RuntimeException("503 from the receiver")).when(publisher).deliver(any());

    relay();
    OutboxEvent next = pending(7L, 0);
    relay();

    assertThat(registry.find(OutboxMetrics.CLAIMS_PAUSED).gauge()).isNull();
    assertThat(next.getAttempts()).isEqualTo(1);
  }

  /** A failure the publisher calls the record's, not the transport's, never pauses. */
  @Test
  void failuresThePublisherDoesNotCallTransportDown_neverPause() {
    transportByDestination();
    doReturn(false).when(publisher).transportDown(any(), any());
    for (long id = 1; id <= 6; id++) {
      overKafka(id);
    }
    doThrow(new RuntimeException("record too large")).when(publisher).deliver(any());

    relay();
    OutboxEvent next = overKafka(7L);
    relay();

    assertThat(next.getAttempts()).isEqualTo(1);
  }

  /** A terminal refusal is the receiver's answer - the transport works: it never pauses. */
  @Test
  void terminalRefusals_neverPause() {
    transportByDestination();
    for (long id = 1; id <= 6; id++) {
      overKafka(id);
    }
    doThrow(new TerminalOutboxException("422 from the receiver")).when(publisher).deliver(any());

    relay();
    OutboxEvent next = overKafka(7L);
    relay();

    assertThat(next.getAttempts()).isEqualTo(1);
  }

  /** {@code claim-pause.after-failures: 0} switches the pause off. */
  @Test
  void afterFailuresZero_neverPauses() {
    properties.getClaimPause().setAfterFailures(0);
    transportByDestination();
    for (long id = 1; id <= 6; id++) {
      overKafka(id);
    }
    doThrow(new RuntimeException("broker unreachable")).when(publisher).deliver(any());

    relay();
    OutboxEvent next = overKafka(7L);
    relay();

    assertThat(next.getAttempts()).isEqualTo(1);
  }

  /**
   * The CONCURRENT lane, paused transport: its keys are passed over, a row of another publisher
   * behind them is taken; the pass that filled no room does not claim again at once.
   */
  @Test
  void aPausedTransportsConcurrentRows_areLeft_andTheRowsBehindThemTaken() {
    transportByDestination();
    for (long id = 1; id <= 6; id++) {
      concurrent(id, "k" + id).setDestination("kafka:topic");
    }
    doThrow(new RuntimeException("broker unreachable")).when(publisher).deliver(any());
    relay(); // two slots: rows 1, 2
    relay(); // rows 3, 4
    relay(); // rows 5, 6: paused
    assertThat(claimsPaused("kafka")).isEqualTo(1d);

    for (long id = 11; id <= 14; id++) {
      concurrent(id, "a" + id).setDestination("kafka:topic"); // keys BEFORE the others
    }
    OutboxEvent other = concurrent(15L, "z15");
    doReturn(null).when(publisher).deliver(other);
    OutboxRelayWorker.Batch pass = worker.relayBatch(false);
    idle(lane, 2);

    assertThat(other.getRelayedOn()).isNotNull();
    assertThat(pass.concurrentFull()).isFalse();
    for (long id = 11; id <= 14; id++) {
      assertThat(table.get(id).getAttempts()).as("row %d", id).isZero();
      assertThat(table.get(id).getClaimedUntil()).as("row %d", id).isNull();
    }
  }

  /**
   * A paused transport's row due for longer than {@code claim-pause.max} is UNATTENDED - no pod
   * sends the transport's rows: it is taken as before 1.11.0 (attempted, backed off), so the
   * paused backlog never hides the rows behind it for longer than that. A fresh one is left.
   */
  @Test
  void aPausedTransportsRowDueLongerThanTheCap_isTakenAsBefore_aFreshOneIsLeft() {
    transportByDestination();
    for (long id = 1; id <= 5; id++) {
      overKafka(id);
    }
    doThrow(new RuntimeException("broker unreachable")).when(publisher).deliver(any());
    relay(); // paused
    OutboxEvent unattended = overKafka(6L);
    Duration cap = properties.getClaimPause().getMax();
    unattended.setNextAttemptOn(OffsetDateTime.now().minus(cap).minusSeconds(1));
    OutboxEvent fresh = overKafka(7L);

    relay();

    assertThat(unattended.getAttempts()).isEqualTo(1); // taken: its backoff moves it aside
    assertThat(fresh.getAttempts()).isZero(); // left for a pod that can send it
    assertThat(fresh.getClaimedUntil()).isNull();
  }

  /**
   * The probe is a FRESH row: a row that already failed, due again, is left for the pods that can
   * send it - a probe on it would pay this pod's failure on the same row at every probe, up to
   * PARKED.
   */
  @Test
  void theProbe_neverTakesARowThatAlreadyFailed() {
    properties.setBackoffBase(Duration.ofMillis(200));
    transportByDestination();
    for (long id = 1; id <= 5; id++) {
      overKafka(id);
    }
    doThrow(new RuntimeException("broker unreachable")).when(publisher).deliver(any());
    relay(); // paused
    OutboxEvent failedBefore = overKafka(6L);
    failedBefore.setAttempts(1);
    await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).until(() -> true);

    relay(); // the probe's time has come - and the only due rows already failed

    assertThat(failedBefore.getAttempts()).isEqualTo(1);
    assertThat(failedBefore.getClaimedUntil()).isNull();
    OutboxEvent fresh = overKafka(7L);
    relay();
    assertThat(fresh.getAttempts()).isEqualTo(1); // the probe, a fresh row
  }

  /**
   * The heartbeat's renewals on the fake table: every lease it renews is kept, its new stamp
   * written - as the guarded statement does while no other holder took the row.
   */
  private void renewalsKept() {
    when(repository.renewSending(any(), any(), any(), any()))
        .thenAnswer(
            inv -> {
              List<Long> ids = idsOf(inv.getArgument(0));
              List<String> renewed = keysOf(inv.getArgument(2));
              for (int i = 0; i < ids.size(); i++) {
                table.get(ids.get(i)).setClaimedUntil(OffsetDateTime.parse(renewed.get(i)));
              }
              return ids;
            });
  }

  /** A send that hangs until its thread is interrupted, then fails as a Kafka client does. */
  private static Object hangUntilInterrupted(CountDownLatch started) {
    started.countDown();
    try {
      new CountDownLatch(1).await();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt(); // the client leaves the flag set
      throw new IllegalStateException("interrupted while waiting for the broker", ex);
    }
    return null;
  }

  /**
   * OUTBOX-INTERRUPTED-SEND-COUNTS-FOR-PAUSE-1: a send over a named transport that outran its
   * bound - the heartbeat interrupted it - counts towards the pause, whatever the publisher makes
   * of the interrupt (Kafka's {@code InterruptException} is neither retriable nor a timeout). Up
   * to 1.11.0 only {@code transportDown} decided, so a transport whose sends hung never paused.
   */
  @Test
  void sendsTheHeartbeatInterruptedAtTheirBound_pauseTheirTransport() {
    Duration bound = Duration.ofMillis(200);
    OutboxSendHeartbeat heartbeat =
        new OutboxSendHeartbeat(repository, tx, OutboxClock.system(), bound);
    OutboxRelayWorker bounded = workerOn(lane, orderedLane, heartbeat);
    renewalsKept();
    transportByDestination();
    doReturn(bound).when(publisher).lease(any());
    doReturn(false).when(publisher).transportDown(any(), any()); // the interrupt: not "down"
    for (long id = 1; id <= 5; id++) {
      overKafka(id);
    }
    doAnswer(inv -> hangUntilInterrupted(new CountDownLatch(1))).when(publisher).deliver(any());

    heartbeat.start();
    try {
      bounded.relayBatch(true);
      orderedIdle();
    } finally {
      heartbeat.stop();
    }

    assertThat(table.values()) // each an ordinary failed attempt, booked by its holder
        .allSatisfy(row -> assertThat(row.getAttempts()).isEqualTo(1));
    assertThat(registry.find(OutboxMetrics.CLAIMS_PAUSED).gauge())
        .as("five sends outran their bound: the transport is paused")
        .isNotNull();
    assertThat(claimsPaused("kafka")).isEqualTo(1d);
  }

  /**
   * An interrupt the heartbeat did not make - the lane's own at shutdown - counts nothing: the
   * send did not outrun its bound, the transport said nothing. Even at {@code after-failures: 1}.
   */
  @Test
  void anInterruptAtShutdown_countsNothingTowardsThePause() {
    properties.getClaimPause().setAfterFailures(1);
    OutboxOrderedLane closing = new OutboxOrderedLane(Duration.ofMillis(100));
    OutboxRelayWorker stopping = workerOn(lane, closing, OutboxSendHeartbeatFixture.idle());
    transportByDestination();
    doReturn(false).when(publisher).transportDown(any(), any());
    OutboxEvent row = overKafka(1L);
    CountDownLatch sending = new CountDownLatch(1);
    doAnswer(inv -> hangUntilInterrupted(sending)).when(publisher).deliver(any());

    stopping.relayBatch(true);
    await().atMost(Duration.ofSeconds(5)).until(() -> sending.getCount() == 0);
    closing.close(); // the grace runs out: the send in flight is interrupted

    await().atMost(Duration.ofSeconds(5)).until(() -> row.getAttempts() == 1);
    assertThat(registry.find(OutboxMetrics.CLAIMS_PAUSED).gauge()).isNull();
  }

  // ------------------------------------------------------------ the relay span covers the round

  /**
   * OUTBOX-A-RELAY-PASS-THAT-MOVES-ROWS-IS-ONE-TRACE-1: a row that stored a {@code traceparent} is
   * relayed inside ONE relay span under the stored parent, opened on the lane's thread around the
   * whole round - the send and the booking with its hook - with the claim's time and the wait
   * before the send on it; a row without one, and a claim that takes nothing, open nothing.
   */
  private static final String TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";

  private static final String STORED_SPAN = "00f067aa0ba902b7";
  private static final String STORED_HEADERS =
      "{\"traceparent\":\"00-" + TRACE + "-" + STORED_SPAN + "-01\"}";

  private final TestTracing tracing = new TestTracing();

  /** What was current on the booking hook's thread when each row was booked. */
  private final Map<Long, String> observedAtBooking = new ConcurrentHashMap<>();

  private OutboxRelayWorker tracedWorker() {
    OutboxRelayWorker traced = workerOn(lane);
    traced.observeRounds(new OutboxRelayObservation(tracing.registry()));
    doAnswer(
            inv -> {
              OutboxEvent booked = inv.getArgument(0);
              Observation current = tracing.registry().getCurrentObservation();
              observedAtBooking.put(
                  booked.getId(), current == null ? "none" : current.getContext().getName());
              bookings.add(inv.getArgument(1));
              return null;
            })
        .when(publisher)
        .onBooked(any(), any());
    return traced;
  }

  private void relayOn(OutboxRelayWorker traced) {
    traced.relayBatch(true);
    orderedIdle();
    idle(lane, 2);
  }

  private SpanData theRelaySpan() {
    await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () ->
                tracing.spans().stream()
                    .anyMatch(span -> span.getName().equals(OutboxRelayObservation.SPAN_NAME)));
    return tracing.span(OutboxRelayObservation.SPAN_NAME);
  }

  @Test
  void aConcurrentRowWithAStoredTraceparent_isRelayedInOneSpanUnderIt_theBookingInside() {
    concurrent(1L, "k1").setHeaders(STORED_HEADERS);

    relayOn(tracedWorker());

    SpanData relay = theRelaySpan();
    assertThat(relay.getTraceId()).as("the stored trace continues").isEqualTo(TRACE);
    assertThat(relay.getParentSpanId()).as("relay span <- stored span").isEqualTo(STORED_SPAN);
    assertThat(relay.getAttributes().asMap().toString())
        .contains(
            "outbox.id=1",
            OutboxRelayObservation.CLAIM_MS + "=",
            OutboxRelayObservation.QUEUED_MS + "=",
            "outbox.transport=" + OutboxRelayObservation.OTHER_TRANSPORT,
            "outbox.destination=comm.delivery.v1");
    assertThat(observedAtBooking)
        .as("the booking ran inside the relay span")
        .containsEntry(1L, OutboxRelayObservation.NAME);
    assertThat(tracing.spans()).as("one span for the round, none for the claim").hasSize(1);
  }

  @Test
  void anOrderedRowWithAStoredTraceparent_isRelayedInOneSpanUnderIt_theBookingInside() {
    pending(1L, 0).setHeaders(STORED_HEADERS);

    relayOn(tracedWorker());

    SpanData relay = theRelaySpan();
    assertThat(relay.getParentSpanId()).isEqualTo(STORED_SPAN);
    assertThat(relay.getAttributes().asMap().toString())
        .contains(OutboxRelayObservation.CLAIM_MS + "=", OutboxRelayObservation.QUEUED_MS + "=");
    assertThat(observedAtBooking).containsEntry(1L, OutboxRelayObservation.NAME);
  }

  @Test
  void aRowWithoutAStoredTraceparent_andAnEmptyClaim_openNoSpan() {
    OutboxRelayWorker traced = tracedWorker();
    relayOn(traced); // nothing due: the sweep of an idle pod

    concurrent(1L, "k1").setHeaders("{\"x-custom\":\"kept\"}");
    pending(2L, 0);
    relayOn(traced);

    assertThat(table.get(1L).getRelayedOn()).isNotNull();
    assertThat(table.get(2L).getRelayedOn()).isNotNull();
    assertThat(tracing.spans()).isEmpty();
    assertThat(observedAtBooking).containsEntry(1L, "none").containsEntry(2L, "none");
  }

  @Test
  void aFailedSend_endsTheRoundsSpanInError_withItsFailureBookedInside() {
    concurrent(1L, "k1").setHeaders(STORED_HEADERS);
    doThrow(new RuntimeException("broker down")).when(publisher).deliver(any());

    relayOn(tracedWorker());

    SpanData relay = theRelaySpan();
    assertThat(relay.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
    assertThat(table.get(1L).getAttempts()).isEqualTo(1);
    assertThat(observedAtBooking).containsEntry(1L, OutboxRelayObservation.NAME);
  }

  // ------------------------------------------------------------ the service's failure text

  /** A peer's answer the service keeps out of its rows and logs. */
  private static final String PEER_BODY = "peer-body-4711";

  /** The service's text: the failure's kind and a fixed word, never the failure's message. */
  private static final OutboxFailureText FIXED_WORD =
      (row, failure) -> OutboxFailureText.typed(failure, "refused by the peer");

  /** The relay's own log lines, captured while {@code run} runs. */
  private static List<ILoggingEvent> relayLog(Runnable run) {
    ListAppender<ILoggingEvent> log = new ListAppender<>();
    log.start();
    Logger logger = (Logger) LoggerFactory.getLogger(OutboxRelayWorker.class);
    logger.addAppender(log);
    try {
      run.run();
    } finally {
      logger.detachAppender(log);
    }
    return log.list;
  }

  /** The kind a view without the sensitive text shows for a stored {@code lastError}. */
  private static String kindOf(String lastError) {
    try {
      Method kindOf = OutboxRowView.class.getDeclaredMethod("kindOf", String.class);
      kindOf.setAccessible(true);
      return (String) kindOf.invoke(null, lastError);
    } catch (ReflectiveOperationException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private static void noPeerBody(List<ILoggingEvent> log) {
    assertThat(log).isNotEmpty();
    assertThat(log)
        .extracting(ILoggingEvent::getFormattedMessage)
        .noneMatch(line -> line.contains(PEER_BODY));
  }

  @Test
  void aRetriedRow_storesTheServicesText_andItsRetryLineCarriesNoPeerBody() {
    worker.describeFailures(FIXED_WORD);
    OutboxEvent event = pending(1L, 0);
    doThrow(new RuntimeException("503 from the peer: " + PEER_BODY)).when(publisher).deliver(event);

    List<ILoggingEvent> log = relayLog(this::relay);

    assertThat(event.getLastError()).isEqualTo("RuntimeException: refused by the peer");
    assertThat(event.getParkedOn()).isNull();
    assertThat(log)
        .extracting(ILoggingEvent::getFormattedMessage)
        .anyMatch(line -> line.endsWith("RuntimeException: refused by the peer"));
    noPeerBody(log);
  }

  @Test
  void aParkedRow_storesTheServicesText_andItsParkLineCarriesNoPeerBody() {
    worker.describeFailures(FIXED_WORD);
    OutboxEvent event = pending(1L, 0);
    doThrow(new TerminalOutboxException("400 from the peer: " + PEER_BODY))
        .when(publisher)
        .deliver(event);

    List<ILoggingEvent> log = relayLog(this::relay);

    assertThat(event.getParkedOn()).isNotNull();
    assertThat(event.getLastError()).isEqualTo("TerminalOutboxException: refused by the peer");
    assertThat(log).extracting(ILoggingEvent::getLevel).contains(Level.ERROR);
    noPeerBody(log);
  }

  @Test
  void aDroppedRow_storesTheServicesText_andItsDropLineCarriesNoPeerBody() {
    worker.describeFailures(FIXED_WORD);
    when(publisher.maxAttempts(any())).thenReturn(1);
    when(publisher.onExhausted(any())).thenReturn(ExhaustionOutcome.DROP);
    OutboxEvent event = pending(1L, 0);
    doThrow(new RuntimeException("410 from the peer: " + PEER_BODY)).when(publisher).deliver(event);

    List<ILoggingEvent> log = relayLog(this::relay);

    assertThat(event.getRelayedOn()).isNotNull();
    assertThat(event.getLastError()).isEqualTo("RuntimeException: refused by the peer");
    noPeerBody(log);
  }

  @Test
  void aRowCancelledInFlightThatFails_storesTheServicesText_andItsLineCarriesNoPeerBody() {
    worker.describeFailures(FIXED_WORD);
    OutboxEvent event = pending(1L, 0);
    doAnswer(
            inv -> {
              event.setCancelledOn(OffsetDateTime.now());
              throw new RuntimeException("503 from the peer: " + PEER_BODY);
            })
        .when(publisher)
        .deliver(event);

    List<ILoggingEvent> log = relayLog(this::relay);

    assertThat(bookings).extracting(OutboxBooking::outcome).containsExactly(Outcome.CANCELLED);
    assertThat(event.getLastError()).isEqualTo("RuntimeException: refused by the peer");
    noPeerBody(log);
  }

  @Test
  void anUnroutableRow_storesTheServicesText() {
    worker.describeFailures(FIXED_WORD);
    when(publisher.supports(any())).thenReturn(false);
    OutboxEvent event = pending(1L, properties.getMaxAttempts() - 1);

    relay();

    assertThat(event.getParkedOn()).isNotNull();
    assertThat(event.getLastError()).isEqualTo("IllegalStateException: refused by the peer");
  }

  @Test
  void theServicesText_isHandedTheRowsFacts_readOnly_withThisAttemptCounted() {
    List<OutboxFailureText.Row> handed = new CopyOnWriteArrayList<>();
    worker.describeFailures(
        (row, failure) -> {
          handed.add(row);
          return "RuntimeException: seen";
        });
    OutboxEvent event = pending(1L, 2);
    event.setAggregateType("Dispatch");
    event.setAggregateId("d-1");
    event.setEventType("DispatchStaged");
    doThrow(new RuntimeException(PEER_BODY)).when(publisher).deliver(event);

    relay();

    assertThat(handed)
        .containsExactly(
            new OutboxFailureText.Row(
                1L, "Dispatch", "d-1", "DispatchStaged", "comm.delivery.v1", 3));
    assertThat(event.getAttempts()).isEqualTo(3);
  }

  @Test
  void aNullOrBlankText_storesTheFailuresTypeNameAlone() {
    OutboxEvent nothing = pending(1L, 0);
    OutboxEvent blank = pending(2L, 0);
    worker.describeFailures((row, failure) -> row.id() == 1L ? null : "  ");
    doThrow(new IllegalStateException(PEER_BODY)).when(publisher).deliver(any());

    relay();

    assertThat(nothing.getLastError()).isEqualTo("IllegalStateException");
    assertThat(blank.getLastError()).isEqualTo("IllegalStateException");
  }

  @Test
  void aTextThatThrows_storesTheFailuresTypeNameAlone_logsNoMessage_andTheRowIsBooked() {
    worker.describeFailures(
        (row, failure) -> {
          throw new IllegalArgumentException("the hook broke on " + PEER_BODY);
        });
    OutboxEvent event = pending(1L, 0);
    doThrow(new RuntimeException("503 from the peer: " + PEER_BODY)).when(publisher).deliver(event);

    List<ILoggingEvent> log = relayLog(this::relay);

    assertThat(event.getAttempts()).isEqualTo(1);
    assertThat(event.getLastError()).isEqualTo("RuntimeException");
    assertThat(event.getClaimedUntil()).isNull();
    assertThat(event.getNextAttemptOn()).isAfter(OffsetDateTime.now());
    assertThat(rolledBack).isZero();
    assertThat(log)
        .extracting(ILoggingEvent::getFormattedMessage)
        .anyMatch(line -> line.contains(IllegalArgumentException.class.getName()));
    noPeerBody(log);
  }

  @Test
  void anOversizedServiceText_isCutToTheColumnsBound() {
    worker.describeFailures((row, failure) -> "RuntimeException: " + "x".repeat(10_000));
    OutboxEvent event = pending(1L, 0);
    doThrow(new RuntimeException("boom")).when(publisher).deliver(event);

    relay();

    assertThat(event.getLastError()).hasSize(OutboxRelayWorker.LAST_ERROR_MAX_LENGTH);
  }

  @Test
  void aServiceTextWithoutAThrowablesName_isBooked_andItsViewShowsUnclassified() {
    worker.describeFailures((row, failure) -> "the peer refused");
    OutboxEvent event = pending(1L, 0);
    doThrow(new RuntimeException(PEER_BODY)).when(publisher).deliver(event);

    relay();

    assertThat(event.getAttempts()).isEqualTo(1);
    assertThat(event.getLastError()).isEqualTo("the peer refused");
    assertThat(kindOf(event.getLastError())).isEqualTo("unclassified");
  }

  @Test
  void aTypedServiceText_keepsTheFailuresKindInTheView() {
    worker.describeFailures(FIXED_WORD);
    OutboxEvent event = pending(1L, 0);
    doThrow(new TerminalOutboxException(PEER_BODY)).when(publisher).deliver(event);

    relay();

    assertThat(kindOf(event.getLastError())).isEqualTo("TerminalOutboxException");
  }

  // ------------------------------------------------------------ a key's run as one unit, 1.12.0

  /** A lane of two slots that claims up to eight rows ahead: room for a key's run. */
  private final OutboxConcurrentLane wide =
      fast(new OutboxConcurrentLane(2, 8, false, Duration.ofSeconds(5)));

  /** The run statements and the guarded release, on the fake table, and a key run of four. */
  private OutboxRelayWorker runs() {
    properties.getConcurrent().setKeyRun(4);
    when(repository.keyRunTails(any(), anyInt()))
        .thenAnswer(
            inv -> {
              int limit = inv.getArgument(1);
              List<OutboxEvent> tails = new ArrayList<>();
              for (long head : idsOf(inv.getArgument(0))) {
                String key = table.get(head).getOrderingKey();
                pendingConcurrent().stream()
                    .filter(e -> key.equals(e.getOrderingKey()) && e.getId() > head)
                    .sorted(Comparator.comparing(OutboxEvent::getId))
                    .limit(limit)
                    .forEach(tails::add);
              }
              return tails;
            });
    when(repository.lockKeyRunTails(any(), any()))
        .thenAnswer(
            inv -> {
              OffsetDateTime now = inv.getArgument(0);
              return idsOf(inv.getArgument(1)).stream()
                  .filter(id -> claimable(table.get(id), now) && !lockedElsewhere.contains(id))
                  .toList();
            });
    when(repository.releaseLeases(any(), any(), any()))
        .thenAnswer(
            inv -> {
              List<Long> ids = idsOf(inv.getArgument(0));
              String inner = ((String) inv.getArgument(1)).replaceAll("[{}\"]", "");
              List<OffsetDateTime> stamps =
                  Stream.of(inner.split(",")).map(OffsetDateTime::parse).toList();
              int released = 0;
              for (int i = 0; i < ids.size(); i++) {
                OutboxEvent row = table.get(ids.get(i));
                if (row.getClaimedUntil() != null && row.getClaimedUntil().isEqual(stamps.get(i))) {
                  row.setClaimedUntil(null);
                  row.setNextAttemptOn(inv.getArgument(2));
                  released++;
                }
              }
              return released;
            });
    return workerOn(wide);
  }

  private void relayRuns(OutboxRelayWorker runWorker) {
    runWorker.relayBatch(false);
    idle(wide, 2);
  }

  @AfterEach
  void closeWideLane() {
    wide.close();
    properties.getConcurrent().setKeyRun(1);
  }

  /** The sends in the order the publisher saw them. */
  private List<Long> recordSends() {
    List<Long> sent = new CopyOnWriteArrayList<>();
    doAnswer(
            inv -> {
              sent.add(inv.<OutboxEvent>getArgument(0).getId());
              return null;
            })
        .when(publisher)
        .deliver(any());
    return sent;
  }

  @Test
  void aKeysRun_isLeasedByOneClaim_sentInIdOrder_andEachRowBookedOnItsOwn() {
    OutboxRelayWorker runWorker = runs();
    concurrent(1L, "k");
    concurrent(2L, "k");
    concurrent(3L, "k");
    List<Long> sent = recordSends();

    relayRuns(runWorker);

    verify(repository, times(1)).stampLeases(any(), eq("{1,2,3}"));
    assertThat(sent).containsExactly(1L, 2L, 3L);
    assertThat(table.values()).allMatch(e -> e.getRelayedOn() != null && e.getClaimedUntil() == null);
    assertThat(bookings).hasSize(3); // one booking per row, each renewing the next
  }

  @Test
  void aKeyRunOfOne_takesOneRowOfAKeyPerClaim_asBefore() {
    OutboxRelayWorker runWorker = runs();
    properties.getConcurrent().setKeyRun(1);
    concurrent(1L, "k");
    concurrent(2L, "k");

    relayRuns(runWorker);

    verify(repository, never()).keyRunTails(any(), anyInt());
    assertThat(table.get(1L).getRelayedOn()).isNotNull();
    assertThat(table.get(2L).getRelayedOn()).isNull(); // the next claim's head
  }

  @Test
  void aFailedRow_endsTheRun_andTheRowsBehindItAreReleased_dueAtOnce_unsent() {
    OutboxRelayWorker runWorker = runs();
    concurrent(1L, "k");
    OutboxEvent failing = concurrent(2L, "k");
    concurrent(3L, "k");
    List<Long> sent = recordSends();
    doAnswer(
            inv -> {
              sent.add(2L);
              throw new IllegalStateException("receiver down");
            })
        .when(publisher)
        .deliver(failing);

    relayRuns(runWorker);

    assertThat(sent).containsExactly(1L, 2L);
    assertThat(table.get(1L).getRelayedOn()).isNotNull();
    assertThat(failing.getAttempts()).isEqualTo(1);
    assertThat(failing.getNextAttemptOn()).isAfter(OffsetDateTime.now()); // backing off
    OutboxEvent behind = table.get(3L);
    assertThat(behind.getClaimedUntil()).isNull(); // released in the failure's booking
    assertThat(behind.getNextAttemptOn()).isBeforeOrEqualTo(OffsetDateTime.now());
    assertThat(behind.getAttempts()).isZero();

    relayRuns(runWorker); // the next claim: row 3 is its key's due head now (row 2 backs off)

    assertThat(sent).containsExactly(1L, 2L, 3L);
  }

  @Test
  void aRowNotDueBehindTheHead_endsTheRun_beforeIt() {
    OutboxRelayWorker runWorker = runs();
    concurrent(1L, "k");
    concurrent(2L, "k").setReleaseAt(OffsetDateTime.now().plusMinutes(5));
    concurrent(3L, "k");

    runWorker.relayBatch(false);

    verify(repository).stampLeases(any(), eq("{1}"));
    verify(repository, never()).lockKeyRunTails(any(), any());
    idle(wide, 2);
  }

  @Test
  void aRowAnotherTransactionHoldsLocked_endsTheRun_beforeIt() {
    OutboxRelayWorker runWorker = runs();
    concurrent(1L, "k");
    concurrent(2L, "k");
    concurrent(3L, "k");
    lockedElsewhere.add(2L); // an operator's cancel, say

    runWorker.relayBatch(false);

    verify(repository).stampLeases(any(), eq("{1}"));
    idle(wide, 2);
  }

  @Test
  void theRoomTheHeadsLeft_boundsTheRuns_overTheClaim() {
    OutboxRelayWorker runWorker = runs();
    for (long id = 1; id <= 6; id++) {
      concurrent(id, "a");
    }
    for (long id = 7; id <= 12; id++) {
      concurrent(id, "b");
    }
    for (long id = 13; id <= 18; id++) {
      concurrent(id, "c");
    }
    List<Long> sent = recordSends();

    int claimed = runWorker.relayBatch(false).concurrent();

    assertThat(claimed).isLessThanOrEqualTo(8); // the lane's room, whatever the runs want
    idle(wide, 2);
    assertThat(sent).containsSubsequence(1L, 2L, 3L).containsSubsequence(7L, 8L);
  }

  @Test
  void aRowCancelledBeforeItsTurn_isReleasedUnsent_andTheRunGoesOnBehindIt() {
    OutboxRelayWorker runWorker = runs();
    OutboxEvent head = concurrent(1L, "k");
    OutboxEvent cancelled = concurrent(2L, "k");
    concurrent(3L, "k");
    List<Long> sent = recordSends();
    doAnswer(
            inv -> {
              sent.add(1L);
              cancelled.setCancelledOn(OffsetDateTime.now()); // the operator was quicker
              return null;
            })
        .when(publisher)
        .deliver(head);

    relayRuns(runWorker);

    assertThat(sent).containsExactly(1L, 3L);
    assertThat(cancelled.getClaimedUntil()).isNull();
    assertThat(cancelled.getRelayedOn()).isNull();
    assertThat(table.get(3L).getRelayedOn()).isNotNull();
  }

  @Test
  void aRowLostToAnotherHolder_endsTheRun_andTheRestIsReleased() {
    OutboxRelayWorker runWorker = runs();
    OutboxEvent head = concurrent(1L, "k");
    OutboxEvent taken = concurrent(2L, "k");
    concurrent(3L, "k");
    List<Long> sent = recordSends();
    doAnswer(
            inv -> {
              sent.add(1L);
              taken.setClaimedUntil(OffsetDateTime.now().plusMinutes(9)); // another holder's
              return null;
            })
        .when(publisher)
        .deliver(head);

    relayRuns(runWorker);

    assertThat(sent).containsExactly(1L);
    assertThat(head.getRelayedOn()).isNotNull();
    assertThat(table.get(3L).getClaimedUntil()).isNull(); // released, due for the next claim
    assertThat(table.get(3L).getRelayedOn()).isNull();
  }

  @Test
  void aHeadWhoseLeaseLapsedBeforeItsBooking_endsTheRun_andTheRestIsReleased() {
    OutboxRelayWorker runWorker = runs();
    OutboxEvent head = concurrent(1L, "k");
    concurrent(2L, "k");
    List<Long> sent = recordSends();
    doAnswer(
            inv -> {
              sent.add(1L);
              head.setClaimedUntil(OffsetDateTime.now().plusMinutes(9)); // lapsed, re-stamped
              return null;
            })
        .when(publisher)
        .deliver(head);

    relayRuns(runWorker);

    assertThat(sent).containsExactly(1L);
    assertThat(head.getRelayedOn()).isNull(); // the other holder books it
    assertThat(table.get(2L).getClaimedUntil()).isNull();
  }

  @Test
  void aRelayedBookingRefusedByAListener_endsTheRun_andTheRestIsReleased() {
    OutboxRelayWorker runWorker = runs();
    concurrent(1L, "k");
    concurrent(2L, "k");
    List<Long> sent = recordSends();
    listeners.add(
        row -> {
          throw new IllegalStateException("listener refuses");
        });

    relayRuns(runWorker);

    assertThat(sent).containsExactly(1L);
    assertThat(table.get(1L).getAttempts()).isEqualTo(1); // booked as a failure instead
    assertThat(table.get(2L).getClaimedUntil()).isNull();
  }

  @Test
  void aHeadWhoseLastSendLapsedUnderWay_runsAlone_andItsLapseIsBooked() {
    OutboxRelayWorker runWorker = runs();
    OutboxEvent head = concurrent(1L, "k");
    head.setClaimedUntil(OffsetDateTime.now().minusSeconds(1));
    head.setSendingSince(OffsetDateTime.now().minusSeconds(5));
    concurrent(2L, "k");
    List<Long> sent = recordSends();

    relayRuns(runWorker);

    verify(repository, never()).keyRunTails(any(), anyInt());
    assertThat(sent).isEmpty();
    assertThat(head.getAttempts()).isEqualTo(1); // the lapse, counted
  }

  @Test
  void aQueuedRunWhoseHeadWaitedAndWasTaken_sendsNothing_andReleasesTheRest() throws Exception {
    runs();
    when(publisher.lease(any())).thenReturn(Duration.ofSeconds(1));
    CountDownLatch gate = new CountDownLatch(1);
    List<Long> delivered = new CopyOnWriteArrayList<>();
    OutboxConcurrentLane oneSlot = oneSlotHeldBy(gate, delivered);
    OutboxRelayWorker queued = workerOn(oneSlot);
    concurrent(1L, null); // holds the one slot
    OutboxEvent head = concurrent(2L, "k");
    concurrent(3L, "k");
    try {
      queued.relayBatch(false); // row 1 sending; the run of rows 2 and 3 waits in the queue
      waitPastATenth();
      head.setClaimedUntil(OffsetDateTime.now().plusMinutes(9)); // taken by another holder
      gate.countDown();
      idle(oneSlot, 1);
    } finally {
      oneSlot.close();
    }

    assertThat(delivered).containsExactly(1L);
    assertThat(table.get(3L).getClaimedUntil()).isNull(); // released, due for the next claim
    assertThat(table.get(3L).getRelayedOn()).isNull();
  }
}
