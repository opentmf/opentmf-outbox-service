package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.querydsl.core.types.Predicate;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opentmf.outbox.internal.OutboxAppended;
import org.opentmf.outbox.internal.OutboxEventRepository;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

/** Prune/unpark/cancel semantics + the TMF630 list's derived-state composition. */
class OutboxMaintenanceServiceTests {

  private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
  private final ApplicationEventPublisher events = mock(ApplicationEventPublisher.class);
  private final OutboxMaintenanceService service =
      new OutboxMaintenanceService(repository, new OutboxProperties(), events);

  private static OutboxEvent row(long id, int attempts, OffsetDateTime relayedOn) {
    OutboxEvent event = new OutboxEvent();
    if (attempts >= 10 && relayedOn == null) {
      event.setParkedOn(OffsetDateTime.now()); // the test rows at the old max are PARKED
    }
    event.setId(id);
    event.setAggregateType("t");
    event.setAggregateId("a-" + id);
    event.setEventType("e.v1");
    event.setDestination("topic");
    event.setPayload("{}");
    event.setAttempts(attempts);
    event.setNextAttemptOn(OffsetDateTime.now());
    event.setCreatedOn(OffsetDateTime.now());
    event.setRelayedOn(relayedOn);
    return event;
  }

  @Test
  void unpark_resetsTheParkedRow_makesItDueNow_andNudgesTheRelay() {
    OutboxEvent parked = row(7L, 10, null);
    parked.setNextAttemptOn(OffsetDateTime.now().plusDays(30)); // deep in the backoff tail
    parked.setReleaseAt(OffsetDateTime.now().minusDays(1));
    when(repository.lockById(7L)).thenReturn(Optional.of(parked));

    service.unpark(7L);

    assertThat(parked.getAttempts()).isZero();
    assertThat(parked.getParkedOn()).isNull(); // the stamp is what the claim predicate reads
    // unpark reschedules the retry slot only - the hold is a separate fact
    assertThat(parked.getReleaseAt()).isBeforeOrEqualTo(OffsetDateTime.now().minusDays(1));
    // due NOW - without this an unparked row keeps its far-future slot and never redelivers
    assertThat(parked.getNextAttemptOn()).isBeforeOrEqualTo(OffsetDateTime.now());
    verify(events).publishEvent(new OutboxAppended(7L));
  }

  @Test
  void unpark_rejectsUnknown_relayed_cancelled_andStillRetryingRows() {
    when(repository.lockById(1L)).thenReturn(Optional.empty());
    assertThatIllegalArgumentException().isThrownBy(() -> service.unpark(1L));

    when(repository.lockById(2L)).thenReturn(Optional.of(row(2L, 10, OffsetDateTime.now())));
    assertThatIllegalStateException()
        .isThrownBy(() -> service.unpark(2L))
        .withMessageContaining("already relayed");

    OutboxEvent cancelled = row(4L, 10, null);
    cancelled.setCancelledOn(OffsetDateTime.now());
    when(repository.lockById(4L)).thenReturn(Optional.of(cancelled));
    assertThatIllegalStateException()
        .isThrownBy(() -> service.unpark(4L))
        .withMessageContaining("cancelled");

    when(repository.lockById(3L)).thenReturn(Optional.of(row(3L, 3, null)));
    assertThatIllegalStateException()
        .isThrownBy(() -> service.unpark(3L))
        .withMessageContaining("not parked");
    verifyNoInteractions(events);
  }

  @Test
  void cancel_stampsAnUnreleasedRow_andNeverNudgesTheRelay() {
    OutboxEvent held = row(5L, 0, null);
    held.setReleaseAt(OffsetDateTime.now().plusDays(1));
    when(repository.lockById(5L)).thenReturn(Optional.of(held));

    service.cancel(5L);

    assertThat(held.getCancelledOn()).isNotNull();
    assertThat(held.getRelayedOn()).isNull();
    verifyNoInteractions(events);
  }

  @Test
  void cancel_isGuardedBothWays_relayedAndAlreadyCancelledAreIllegalStates() {
    when(repository.lockById(1L)).thenReturn(Optional.empty());
    assertThatIllegalArgumentException().isThrownBy(() -> service.cancel(1L));

    // the effect has LEFT - a cancel cannot recall it, and must not mark it cancelled either
    OutboxEvent relayed = row(2L, 1, OffsetDateTime.now());
    when(repository.lockById(2L)).thenReturn(Optional.of(relayed));
    assertThatIllegalStateException()
        .isThrownBy(() -> service.cancel(2L))
        .withMessageContaining("already relayed");
    assertThat(relayed.getCancelledOn()).isNull();

    OutboxEvent cancelled = row(3L, 0, null);
    OffsetDateTime firstCancel = OffsetDateTime.now().minusHours(1);
    cancelled.setCancelledOn(firstCancel);
    when(repository.lockById(3L)).thenReturn(Optional.of(cancelled));
    assertThatIllegalStateException()
        .isThrownBy(() -> service.cancel(3L))
        .withMessageContaining("already cancelled");
    assertThat(cancelled.getCancelledOn()).isEqualTo(firstCancel); // the audit stamp stands
  }

  @Test
  @SuppressWarnings("unchecked")
  void list_composesTheDerivedStateFilter_andOmitsPayloads() {
    when(repository.findAll(any(Predicate.class), any(Pageable.class)))
        .thenReturn(new PageImpl<>(List.of(row(1L, 10, null), row(2L, 0, null))));

    var page = service.list(null, OutboxStateFilter.PARKED, PageRequest.of(0, 20));

    ArgumentCaptor<Predicate> predicate = ArgumentCaptor.forClass(Predicate.class);
    verify(repository).findAll(predicate.capture(), any(Pageable.class));
    // parked = parked_on stamped AND not cancelled - both legs present
    assertThat(predicate.getValue().toString())
        .contains("parkedOn is not null")
        .contains("cancelledOn is null");
    assertThat(page.getContent().get(0).parked()).isTrue();
    assertThat(page.getContent()).allSatisfy(v -> assertThat(v.payload()).isNull());
  }

  @Test
  @SuppressWarnings("unchecked")
  void list_coversEveryStateLeg_andTheStatelessCall() {
    when(repository.findAll(any(Predicate.class), any(Pageable.class)))
        .thenReturn(new PageImpl<>(List.of(row(1L, 0, null))));
    ArgumentCaptor<Predicate> predicate = ArgumentCaptor.forClass(Predicate.class);

    service.list(
        QOutboxEvent.outboxEvent.eventType.eq("e.v1"),
        OutboxStateFilter.PENDING,
        PageRequest.of(0, 20));
    service.list(null, OutboxStateFilter.RELAYED, PageRequest.of(0, 20));
    service.list(QOutboxEvent.outboxEvent.eventType.eq("e.v1"), null, PageRequest.of(0, 20));
    service.list(null, OutboxStateFilter.CANCELLED, PageRequest.of(0, 20));

    verify(repository, times(4))
        .findAll(predicate.capture(), any(Pageable.class));
    // pending EXCLUDES cancelled rows (cancelled is a terminal state, not a pending one)
    assertThat(predicate.getAllValues().get(0).toString())
        .contains("eventType = e.v1")
        .contains("relayedOn is null")
        .contains("cancelledOn is null");
    assertThat(predicate.getAllValues().get(1).toString())
        .contains("relayedOn is not null")
        .doesNotContain("cancelledOn");
    assertThat(predicate.getAllValues().get(2).toString())
        .contains("eventType = e.v1")
        .doesNotContain("relayedOn")
        .doesNotContain("cancelledOn");
    assertThat(predicate.getAllValues().get(3).toString())
        .contains("cancelledOn is not null")
        .doesNotContain("relayedOn");
  }

  @Test
  void prune_deletesInBatches_relayedThenCancelled_untilABatchComesBackShort() {
    when(repository.deleteRelayedBatch(any(OffsetDateTime.class), anyInt()))
        .thenReturn(5_000, 5_000, 1_200);
    when(repository.deleteCancelledBatch(any(OffsetDateTime.class), anyInt())).thenReturn(7);

    OutboxPruneResult result = service.pruneExpired();

    assertThat(result.relayed()).isEqualTo(11_200L);
    assertThat(result.cancelled()).isEqualTo(7L);
    assertThat(result.pruned()).isEqualTo(11_207L);
    assertThat(result.moreToPrune()).isFalse();
    // every batch of both legs uses the SAME cutoff (one retention) and the configured size
    ArgumentCaptor<OffsetDateTime> cutoff = ArgumentCaptor.forClass(OffsetDateTime.class);
    verify(repository, times(3)).deleteRelayedBatch(cutoff.capture(), eq(5_000));
    verify(repository).deleteCancelledBatch(cutoff.getValue(), 5_000);
    assertThat(cutoff.getAllValues()).containsOnly(cutoff.getValue());
    assertThat(cutoff.getValue())
        .isBetween(
            OffsetDateTime.now().minusDays(7).minusMinutes(1), OffsetDateTime.now().minusDays(7));
  }

  @Test
  void prune_stopsOnItsTimeBudget_andSaysMoreRemain() {
    OutboxProperties properties = new OutboxProperties();
    properties.getMaintenance().setTimeBudget(Duration.ZERO); // spent before the first batch
    OutboxMaintenanceService bounded =
        new OutboxMaintenanceService(repository, properties, events);

    OutboxPruneResult result = bounded.pruneExpired();

    assertThat(result.pruned()).isZero();
    assertThat(result.moreToPrune()).isTrue();
    verifyNoInteractions(repository);
  }

  @Test
  void prune_stopsMidwayOnItsTimeBudget_withTheRowsSoFar() {
    OutboxProperties properties = new OutboxProperties();
    properties.getMaintenance().setTimeBudget(Duration.ofMillis(200));
    OutboxMaintenanceService bounded =
        new OutboxMaintenanceService(repository, properties, events);
    when(repository.deleteRelayedBatch(any(OffsetDateTime.class), anyInt()))
        .thenAnswer(
            inv -> {
              assertThat(new CountDownLatch(1).await(60, TimeUnit.MILLISECONDS)).isFalse();
              return 5_000; // always a full batch: the backlog never ends within the budget
            });

    OutboxPruneResult result = bounded.pruneExpired();

    assertThat(result.relayed()).isPositive().isLessThan(50_000L);
    assertThat(result.moreToPrune()).isTrue();
    verify(repository, never()).deleteCancelledBatch(any(), anyInt());
  }

  @Test
  void prune_andItsOldName_answerTheCount() {
    when(repository.deleteRelayedBatch(any(OffsetDateTime.class), anyInt())).thenReturn(3);
    when(repository.deleteCancelledBatch(any(OffsetDateTime.class), anyInt())).thenReturn(2);

    assertThat(service.prune()).isEqualTo(5L);
    assertThat(service.pruneRelayed()).isEqualTo(5L); // the 1.0.0 name, same pass
  }

  @Test
  void unparkByFilter_needsADestination_thereIsNoUnparkOfEverything() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> service.unpark(null, null, null, null))
        .withMessageContaining("destination");
    assertThatIllegalArgumentException().isThrownBy(() -> service.unpark(" ", null, null, null));
    verifyNoInteractions(repository, events);
  }

  @Test
  void unparkByFilter_runsBatchesUntilOneComesBackShort_andNudgesTheRelayOnce() {
    when(repository.unparkBatch(any(), eq("hub:a"), any(), any(), eq(5_000)))
        .thenReturn(5_000, 120);

    OutboxUnparkResult result = service.unpark("hub:a", null, null, null);

    assertThat(result.unparked()).isEqualTo(5_120L);
    assertThat(result.moreToUnpark()).isFalse();
    // open ends become far bounds - one plain range, never an OR on a parameter
    ArgumentCaptor<OffsetDateTime> from = ArgumentCaptor.forClass(OffsetDateTime.class);
    ArgumentCaptor<OffsetDateTime> to = ArgumentCaptor.forClass(OffsetDateTime.class);
    verify(repository, times(2))
        .unparkBatch(any(), eq("hub:a"), from.capture(), to.capture(), eq(5_000));
    assertThat(from.getValue()).isBefore(OffsetDateTime.parse("1971-01-01T00:00:00Z"));
    assertThat(to.getValue()).isAfter(OffsetDateTime.parse("9000-01-01T00:00:00Z"));
    verify(events, times(1)).publishEvent(new OutboxAppended(0));
  }

  @Test
  void unparkByFilter_withARangeAndAReference_usesTheNarrowStatement() {
    OffsetDateTime from = OffsetDateTime.now().minusHours(2);
    OffsetDateTime to = OffsetDateTime.now();
    when(repository.unparkBatchByReference(any(), any(), any(), any(), any(), anyInt()))
        .thenReturn(3);

    OutboxUnparkResult result = service.unpark("hub:a", from, to, "sub-7");

    assertThat(result.unparked()).isEqualTo(3L);
    verify(repository)
        .unparkBatchByReference(any(), eq("hub:a"), eq(from), eq(to), eq("sub-7"), eq(5_000));
    verify(repository, never()).unparkBatch(any(), any(), any(), any(), anyInt());
  }

  @Test
  void unparkByFilter_nothingMatching_isQuiet_noNudge() {
    OutboxUnparkResult result = service.unpark("hub:a", null, null, null);

    assertThat(result.unparked()).isZero();
    assertThat(result.moreToUnpark()).isFalse();
    verifyNoInteractions(events);
  }

  @Test
  void unparkByFilter_stopsOnItsTimeBudget_andSaysMoreRemain() {
    OutboxProperties properties = new OutboxProperties();
    properties.getMaintenance().setTimeBudget(Duration.ZERO);

    OutboxUnparkResult result =
        new OutboxMaintenanceService(repository, properties, events)
            .unpark("hub:a", null, null, null);

    assertThat(result.moreToUnpark()).isTrue();
    verifyNoInteractions(repository, events);
  }

  @Test
  void inspect_carriesPayloadAndForensics() {
    OutboxEvent parked = row(7L, 10, null);
    parked.setLastError("boom");
    when(repository.findById(7L)).thenReturn(Optional.of(parked));

    OutboxRowView view = service.inspect(7L);

    assertThat(view.payload()).isEqualTo("{}");
    assertThat(view.lastError()).isEqualTo("boom");
    assertThat(view.parked()).isTrue();
    assertThat(view.parkedOn()).isNotNull();
    assertThat(view.releaseAt()).isNull();
    assertThat(view.cancelledOn()).isNull();
    assertThat(view.reference()).isNull();
  }

  @Test
  void inspect_aCancelledRowAtMaxAttempts_isCancelledNotParked() {
    OutboxEvent cancelled = row(8L, 10, null);
    cancelled.setCancelledOn(OffsetDateTime.now());
    when(repository.findById(8L)).thenReturn(Optional.of(cancelled));

    OutboxRowView view = service.inspect(8L);

    assertThat(view.cancelledOn()).isNotNull();
    assertThat(view.parked()).isFalse(); // terminal beats the derived sub-state
  }

  @Test
  void inspect_unknownRow_isAnIllegalArgument() {
    when(repository.findById(99L)).thenReturn(Optional.empty());
    assertThatIllegalArgumentException()
        .isThrownBy(() -> service.inspect(99L))
        .withMessageContaining("99");
  }
}
