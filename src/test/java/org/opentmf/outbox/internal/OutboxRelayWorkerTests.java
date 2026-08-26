package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.OutboxRelayedListener;
import org.springframework.data.domain.Limit;

/** Success stamps relayed_on; failure books attempts+backoff; park at max-attempts. */
class OutboxRelayWorkerTests {

  private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
  private final OutboxPublisherRouter router = mock(OutboxPublisherRouter.class);
  private final OutboxProperties properties = new OutboxProperties();
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final List<OutboxRelayedListener> listeners = new ArrayList<>();
  private final OutboxRelayWorker worker =
      new OutboxRelayWorker(
          repository,
          router,
          new OutboxBackoff(properties),
          new OutboxMetrics(registry, repository, properties),
          properties,
          listeners);

  private static OutboxEvent pending(long id, int attempts) {
    OutboxEvent event = new OutboxEvent();
    event.setId(id);
    event.setDestination("comm.delivery.v1");
    event.setAttempts(attempts);
    event.setNextAttemptOn(OffsetDateTime.now().minusSeconds(1));
    return event;
  }

  @Test
  void successfulRelay_stampsRelayedOn() {
    OutboxEvent event = pending(1L, 0);
    when(repository.claimBatch(any(), anyInt(), any(Limit.class))).thenReturn(List.of(event));

    int claimed = worker.relayBatch();

    assertThat(claimed).isEqualTo(1);
    assertThat(event.getRelayedOn()).isNotNull();
    // the relay BOOKS the success: counter by destination, attempts = tries taken (0+1)
    assertThat(
            registry
                .get(OutboxMetrics.RELAYED)
                .tag(OutboxMetrics.TAG_DESTINATION, "comm.delivery.v1")
                .counter()
                .count())
        .isEqualTo(1d);
    assertThat(registry.get(OutboxMetrics.ATTEMPTS).summary().totalAmount()).isEqualTo(1d);
  }

  @Test
  void failure_booksAttemptAndBackoff_aFailedRowDoesNotStopTheBatch() {
    OutboxEvent failing = pending(1L, 0);
    OutboxEvent fine = pending(2L, 0);
    when(repository.claimBatch(any(), anyInt(), any(Limit.class)))
        .thenReturn(List.of(failing, fine));
    doThrow(new RuntimeException("broker down")).when(router).publish(failing);

    worker.relayBatch();

    assertThat(failing.getRelayedOn()).isNull();
    assertThat(failing.getAttempts()).isEqualTo(1);
    // EXACT format: SimpleName + message (toString would carry the package prefix)
    assertThat(failing.getLastError()).isEqualTo("RuntimeException: broker down");
    assertThat(failing.getNextAttemptOn()).isAfter(OffsetDateTime.now());
    assertThat(fine.getRelayedOn()).isNotNull();
  }

  @Test
  void relayedListeners_runInsideTheSuccessPath_withTheStampVisible() {
    OutboxEvent event = pending(1L, 0);
    when(repository.claimBatch(any(), anyInt(), any(Limit.class))).thenReturn(List.of(event));
    List<Object> seenRelayedOn = new ArrayList<>();
    listeners.add(e -> seenRelayedOn.add(e.getRelayedOn()));

    worker.relayBatch();

    // the listener saw relayedOn ALREADY SET - the atomic-with-the-stamp contract
    assertThat(seenRelayedOn).hasSize(1);
    assertThat(seenRelayedOn.get(0)).isNotNull();
    assertThat(event.getRelayedOn()).isNotNull();
  }

  @Test
  void aThrowingListener_undoesTheStamp_andBooksAnOrdinaryFailure() {
    OutboxEvent event = pending(1L, 0);
    when(repository.claimBatch(any(), anyInt(), any(Limit.class))).thenReturn(List.of(event));
    listeners.add(
        e -> {
          throw new IllegalStateException("bookkeeping refused");
        });

    worker.relayBatch();

    // NOT relayed-with-attempts++ nonsense: the stamp is undone, the failure books normally
    // and the publish will repeat (at-least-once - the destination dedups on the key)
    assertThat(event.getRelayedOn()).isNull();
    assertThat(event.getAttempts()).isEqualTo(1);
    assertThat(event.getLastError()).isEqualTo("IllegalStateException: bookkeeping refused");
    // the success metric was NOT booked for the failed round
    assertThat(registry.find(OutboxMetrics.RELAYED).counters()).isEmpty();
  }

  @Test
  void theFinalFailedAttempt_parksTheRow() {
    OutboxEvent event = pending(1L, properties.getMaxAttempts() - 1);
    when(repository.claimBatch(any(), anyInt(), any(Limit.class))).thenReturn(List.of(event));
    doThrow(new RuntimeException("still down")).when(router).publish(event);

    worker.relayBatch();

    // attempts now AT max = parked: excluded from claims, gauge alerts, unpark is explicit
    assertThat(event.getAttempts()).isEqualTo(properties.getMaxAttempts());
    assertThat(event.getRelayedOn()).isNull();
  }

  @Test
  void aMessagelessException_isDescribedByItsToString() {
    OutboxEvent event = pending(1L, 0);
    when(repository.claimBatch(any(), anyInt(), any(Limit.class))).thenReturn(List.of(event));
    doThrow(new RuntimeException()).when(router).publish(event);

    worker.relayBatch();

    assertThat(event.getLastError()).contains("RuntimeException");
  }

  @Test
  void anOversizedErrorMessage_isTruncatedForTheForensicColumn() {
    OutboxEvent event = pending(1L, 0);
    when(repository.claimBatch(any(), anyInt(), any(Limit.class))).thenReturn(List.of(event));
    doThrow(new RuntimeException("x".repeat(10_000))).when(router).publish(event);

    worker.relayBatch();

    assertThat(event.getLastError()).hasSize(OutboxRelayWorker.LAST_ERROR_MAX_LENGTH);
  }
}
