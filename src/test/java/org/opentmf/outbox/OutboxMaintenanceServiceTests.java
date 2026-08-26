package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.querydsl.core.types.Predicate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
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
    // parked = pending AND attempts >= max-attempts (default 10) - all legs present
    assertThat(predicate.getValue().toString())
        .contains("relayedOn is null")
        .contains("cancelledOn is null")
        .contains("attempts >= 10");
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
  void prune_deletesRelayedAndCancelledRowsPastRetention_quietWhenNothingQualifies() {
    when(repository.deleteByRelayedOnBefore(any(OffsetDateTime.class))).thenReturn(3L);
    when(repository.deleteByCancelledOnBefore(any(OffsetDateTime.class))).thenReturn(2L);
    assertThat(service.prune()).isEqualTo(5L);
    // both legs use the SAME cutoff - one retention for terminal rows
    ArgumentCaptor<OffsetDateTime> cutoff = ArgumentCaptor.forClass(OffsetDateTime.class);
    verify(repository).deleteByRelayedOnBefore(cutoff.capture());
    verify(repository).deleteByCancelledOnBefore(cutoff.getValue());

    when(repository.deleteByRelayedOnBefore(any(OffsetDateTime.class))).thenReturn(0L);
    when(repository.deleteByCancelledOnBefore(any(OffsetDateTime.class))).thenReturn(0L);
    assertThat(service.pruneRelayed()).isZero(); // the 1.0.0 name, same pass
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
    assertThat(view.releaseAt()).isNull();
    assertThat(view.cancelledOn()).isNull();
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
