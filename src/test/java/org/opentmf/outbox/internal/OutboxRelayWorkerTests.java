package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.Column;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxBooking;
import org.opentmf.outbox.OutboxBooking.Outcome;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxPublisher.ExhaustionOutcome;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.opentmf.outbox.OutboxRelayedListener;
import org.opentmf.outbox.TerminalOutboxException;
import org.springframework.beans.BeanUtils;
import org.springframework.data.domain.Limit;
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
  private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
  private final OutboxPublisher publisher = mock(OutboxPublisher.class, CALLS_REAL_METHODS);
  private final OutboxProperties properties = new OutboxProperties();
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final List<OutboxRelayedListener> listeners = new ArrayList<>();
  private final List<OutboxBooking> bookings = new CopyOnWriteArrayList<>();
  private final OutboxConcurrentLane lane =
      new OutboxConcurrentLane(2, false, Duration.ofSeconds(5));
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
          } catch (RuntimeException ex) {
            rolledBack++;
            before.forEach((id, row) -> BeanUtils.copyProperties(row, table.get(id)));
            throw ex;
          }
        }
      };

  private final OutboxRelayWorker worker =
      new OutboxRelayWorker(
          repository,
          new OutboxPublisherRouter(List.of(publisher)),
          new OutboxBackoff(properties),
          new OutboxMetrics(registry, repository),
          properties,
          listeners,
          tx,
          lane);

  OutboxRelayWorkerTests() {
    when(publisher.supports(any())).thenReturn(true);
    doThrow(new AssertionError("unstubbed publish")).when(publisher).publish(any());
    doReturn(null).when(publisher).deliver(any()); // delivered, no result - per test overridden
    // the two lane claims, emulated on the fake table (the real SQL is the ITs' job)
    when(repository.claimOrdered(any(), any(), any(Limit.class)))
        .thenAnswer(
            inv -> {
              OffsetDateTime now = inv.getArgument(0);
              Limit limit = inv.getArgument(2);
              return table.values().stream()
                  .filter(e -> e.getLane() != Lane.CONCURRENT && claimable(e, now))
                  .limit(limit.max())
                  .toList();
            });
    when(repository.claimConcurrent(any(), any(), any(Limit.class)))
        .thenAnswer(
            inv -> {
              OffsetDateTime now = inv.getArgument(0);
              Limit limit = inv.getArgument(2);
              return table.values().stream()
                  .filter(
                      e ->
                          e.getLane() == Lane.CONCURRENT
                              && claimable(e, now)
                              && keyFree(e, now))
                  .limit(limit.max())
                  .toList();
            });
    when(repository.lockLeased(anyLong(), any()))
        .thenAnswer(
            inv -> {
              OutboxEvent row = table.get(inv.<Long>getArgument(0));
              return Optional.ofNullable(row)
                  .filter(e -> Objects.equals(e.getClaimedUntil(), inv.getArgument(1)));
            });
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
  }

  private static boolean claimable(OutboxEvent e, OffsetDateTime now) {
    return e.getRelayedOn() == null
        && e.getCancelledOn() == null
        && e.getParkedOn() == null
        && (e.getReleaseAt() == null || !e.getReleaseAt().isAfter(now))
        && !e.getNextAttemptOn().isAfter(now)
        && (e.getClaimedUntil() == null || !e.getClaimedUntil().isAfter(now));
  }

  /** The ordering-key rule of the CONCURRENT claim: no lower row of the key due, none flying. */
  private boolean keyFree(OutboxEvent e, OffsetDateTime now) {
    if (e.getOrderingKey() == null) {
      return true;
    }
    return table.values().stream()
        .filter(
            o -> o.getLane() == Lane.CONCURRENT && e.getOrderingKey().equals(o.getOrderingKey()))
        .noneMatch(
            o ->
                (o.getId() < e.getId() && claimable(o, now))
                    || (o.getRelayedOn() == null
                        && o.getCancelledOn() == null
                        && o.getClaimedUntil() != null
                        && o.getClaimedUntil().isAfter(now)));
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

  /** One batch, then wait for the CONCURRENT lane to return every slot. */
  private int relay() {
    int claimed = worker.relayBatch();
    await().atMost(Duration.ofSeconds(5)).until(() -> lane.available() == 2);
    return claimed;
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
    // claim + one booking: two SHORT transactions, the send in neither
    assertThat(committed).isEqualTo(2);
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

  @Test
  void aRefusingHookOnAFailure_booksNothing_theLeaseLapses() {
    OutboxEvent event = pending(1L, 0);
    doThrow(new RuntimeException("receiver 503")).when(publisher).deliver(event);
    doThrow(new IllegalStateException("hook refused"))
        .when(publisher)
        .onBooked(any(), any());

    relay(); // logged, never thrown out of the pass

    assertThat(event.getAttempts()).isZero(); // rolled back
    assertThat(event.getClaimedUntil()).isNotNull(); // still leased: it lapses, then repeats
    assertThat(rolledBack).isEqualTo(1);
  }

  @Test
  void aRefusingHookOnTheFirstOrderedRow_stopsTheBatch_theRestLapse() {
    OutboxEvent first = pending(1L, 0);
    OutboxEvent second = pending(2L, 0);
    doThrow(new RuntimeException("down")).when(publisher).deliver(first);
    doThrow(new IllegalStateException("hook refused"))
        .when(publisher)
        .onBooked(any(), any());

    relay();

    verify(publisher, never()).deliver(second); // the relay thread stopped the batch
    assertThat(second.getClaimedUntil()).isNotNull();
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
    verify(repository, times(1)).lockLeased(anyLong(), any());
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

    // own guard x3 + one renewal for row 2 and one for row 3 - never two rows ahead
    verify(repository, times(5)).lockLeased(anyLong(), any());
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
        .anyMatch(t -> t.startsWith("1@") && !t.contains("opentmf-outbox-send-"))
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
  void theClaim_takesOnlyAsManyConcurrentRowsAsThereAreFreeSlots() {
    concurrent(1L, null);
    concurrent(2L, null);
    OutboxEvent third = concurrent(3L, null);
    CountDownLatch gate = new CountDownLatch(1);
    doAnswer(
            inv -> {
              gate.await();
              return null;
            })
        .when(publisher)
        .deliver(any());

    assertThat(worker.relayBatch()).isEqualTo(2); // two slots
    assertThat(third.getClaimedUntil()).isNull(); // never stamped: no lease burns in a queue
    gate.countDown();
    await().atMost(Duration.ofSeconds(5)).until(() -> lane.available() == 2);
    assertThat(relay()).isEqualTo(1);
    await().atMost(Duration.ofSeconds(5)).until(() -> third.getRelayedOn() != null);
  }

  @Test
  void noFreeSlot_meansNoConcurrentClaimAtAll() {
    OutboxEvent row = concurrent(1L, null);
    assertThat(lane.tryAcquire()).isTrue();
    assertThat(lane.tryAcquire()).isTrue();

    assertThat(worker.relayBatch()).isZero();

    verify(repository, never()).claimConcurrent(any(), any(), any(Limit.class));
    assertThat(row.getClaimedUntil()).isNull();
    lane.releaseUnused(2);
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
  void aConcurrentRowRacingAClaimThatDoesNotCommit_returnsItsSlots() {
    concurrent(1L, null);
    failCommits = true;

    assertThatThrownBy(worker::relayBatch).hasMessageContaining("database gone");

    assertThat(lane.available()).isEqualTo(2);
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

    assertThat(worker.relayBatch()).isEqualTo(1);

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

    worker.relayBatch();

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

    worker.relayBatch();

    assertThat(ordered.getRelayedOn()).isNotNull();
    gate.countDown();
    await().atMost(Duration.ofSeconds(5)).until(() -> lane.available() == 2);
  }
}
