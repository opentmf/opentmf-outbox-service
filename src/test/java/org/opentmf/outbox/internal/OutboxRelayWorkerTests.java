package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.Column;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxRelayedListener;
import org.opentmf.outbox.TerminalOutboxException;
import org.springframework.data.domain.Limit;

/**
 * Success stamps relayed_on; failure books attempts + the publisher's backoff; exhaustion by the
 * publisher's policy: PARK stamps parked_on, DROP stamps relayed_on without a delivery.
 */
class OutboxRelayWorkerTests {

  private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
  private final OutboxPublisher publisher = mock(OutboxPublisher.class);
  private final OutboxProperties properties = new OutboxProperties();
  private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
  private final List<OutboxRelayedListener> listeners = new ArrayList<>();
  private final OutboxRelayWorker worker =
      new OutboxRelayWorker(
          repository,
          new OutboxPublisherRouter(List.of(publisher)),
          new OutboxBackoff(properties),
          new OutboxMetrics(registry, repository),
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

  /** A mock publisher with the DEFAULT policy (Mockito returns 0/null/null otherwise). */
  private void supportsWithDefaultPolicy() {
    when(publisher.supports(any())).thenReturn(true);
    when(publisher.onExhausted(any())).thenReturn(OutboxPublisher.ExhaustionOutcome.PARK);
    when(publisher.backoff(any(), any(Integer.class))).thenReturn(null); // Mockito would say ZERO
  }

  private void claims(OutboxEvent... events) {
    when(repository.claimBatch(any(), any(Limit.class))).thenReturn(List.of(events));
  }

  private double counter(String name) {
    return registry.find(name).counters().stream().mapToDouble(Counter::count).sum();
  }

  @Test
  void successfulRelay_stampsRelayedOn() {
    supportsWithDefaultPolicy();
    OutboxEvent event = pending(1L, 0);
    claims(event);

    int claimed = worker.relayBatch();

    assertThat(claimed).isEqualTo(1);
    assertThat(event.getRelayedOn()).isNotNull();
    assertThat(event.getParkedOn()).isNull();
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
    supportsWithDefaultPolicy();
    OutboxEvent failing = pending(1L, 0);
    OutboxEvent fine = pending(2L, 0);
    claims(failing, fine);
    doThrow(new RuntimeException("broker down")).when(publisher).publish(failing);

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
    supportsWithDefaultPolicy();
    OutboxEvent event = pending(1L, 0);
    claims(event);
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
    supportsWithDefaultPolicy();
    OutboxEvent event = pending(1L, 0);
    claims(event);
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

  /**
   * THE named regression for the scheduled-send hold: a delivery failure's backoff reschedules
   * {@code next_attempt_on} and must NEVER move {@code release_at} - repurposing the hold as
   * the retry slot would release a scheduled send early on its first failure.
   */
  @Test
  void backoff_neverTouchesTheReleaseHold() throws NoSuchFieldException {
    supportsWithDefaultPolicy();
    OffsetDateTime hold = OffsetDateTime.now().minusSeconds(1); // released, so claimable
    OutboxEvent event = pending(1L, 0);
    event.setReleaseAt(hold);
    claims(event);
    doThrow(new RuntimeException("broker down")).when(publisher).publish(event);

    worker.relayBatch();

    assertThat(event.getNextAttemptOn()).isAfter(OffsetDateTime.now()); // backoff booked...
    assertThat(event.getReleaseAt()).isEqualTo(hold); // ...the hold untouched
    // and structurally: the mapping itself refuses to UPDATE the column, whatever the code does
    assertThat(OutboxEvent.class.getDeclaredField("releaseAt").getAnnotation(Column.class))
        .extracting(Column::updatable)
        .isEqualTo(false);
  }

  @Test
  void theFinalFailedAttempt_parksTheRow_byStampingParkedOn() {
    supportsWithDefaultPolicy();
    OutboxEvent event = pending(1L, properties.getMaxAttempts() - 1);
    claims(event);
    doThrow(new RuntimeException("still down")).when(publisher).publish(event);

    worker.relayBatch();

    // attempts now AT the library max = PARKED: parked_on stamped (the claim predicate reads
    // the stamp, not the count), gauge alerts, unpark is explicit
    assertThat(event.getAttempts()).isEqualTo(properties.getMaxAttempts());
    assertThat(event.getParkedOn()).isNotNull();
    assertThat(event.getRelayedOn()).isNull();
  }

  @Test
  void aPublishersOwnBudgetAndBackoff_areHonoured() {
    supportsWithDefaultPolicy();
    when(publisher.maxAttempts(any())).thenReturn(3);
    when(publisher.backoff(any(), any(Integer.class))).thenReturn(Duration.ofHours(5));
    OutboxEvent retrying = pending(1L, 0);
    OutboxEvent lastChance = pending(2L, 2); // library max is 10 - the publisher says 3
    claims(retrying, lastChance);
    doThrow(new RuntimeException("hub 503")).when(publisher).publish(any());

    worker.relayBatch();

    assertThat(retrying.getParkedOn()).isNull();
    assertThat(retrying.getNextAttemptOn()).isAfter(OffsetDateTime.now().plusHours(4));
    assertThat(lastChance.getAttempts()).isEqualTo(3);
    assertThat(lastChance.getParkedOn()).isNotNull(); // exhausted at THE PUBLISHER'S 3
  }

  @Test
  void dropOutcome_stampsRelayedOn_firesNoListener_countsDroppedNotRelayed() {
    supportsWithDefaultPolicy();
    when(publisher.maxAttempts(any())).thenReturn(1);
    when(publisher.onExhausted(any())).thenReturn(OutboxPublisher.ExhaustionOutcome.DROP);
    OutboxEvent event = pending(1L, 0);
    claims(event);
    doThrow(new RuntimeException("hub 410 gone")).when(publisher).publish(event);
    List<Long> listened = new ArrayList<>();
    listeners.add(e -> listened.add(e.getId()));

    worker.relayBatch();

    assertThat(event.getRelayedOn()).isNotNull(); // leaves the pending set...
    assertThat(event.getParkedOn()).isNull();
    // ...forensics kept
    assertThat(event.getLastError()).isEqualTo("RuntimeException: hub 410 gone");
    assertThat(listened).isEmpty(); // nothing was delivered - no bookkeeping seam
    assertThat(counter(OutboxMetrics.DROPPED)).isEqualTo(1d);
    assertThat(counter(OutboxMetrics.RELAYED)).isZero();
  }

  @Test
  void aTerminalException_reachesTheExhaustionOutcomeImmediately() {
    supportsWithDefaultPolicy();
    OutboxEvent event = pending(1L, 0); // first attempt, budget of 10 untouched
    claims(event);
    doThrow(new TerminalOutboxException("400 bad request - retrying is pointless"))
        .when(publisher)
        .publish(event);

    worker.relayBatch();

    assertThat(event.getAttempts()).isEqualTo(1);
    assertThat(event.getParkedOn()).isNotNull(); // PARK by default, on the FIRST attempt
    assertThat(event.getLastError()).contains("retrying is pointless");
  }

  @Test
  void anUnroutableRow_booksWithTheLibraryPolicy() {
    when(publisher.supports(any())).thenReturn(false);
    OutboxEvent event = pending(1L, properties.getMaxAttempts() - 1);
    claims(event);

    worker.relayBatch();

    assertThat(event.getLastError()).contains("No OutboxPublisher supports");
    assertThat(event.getParkedOn()).isNotNull(); // library max, library PARK
  }

  @Test
  void aMessagelessException_isDescribedByItsToString() {
    supportsWithDefaultPolicy();
    OutboxEvent event = pending(1L, 0);
    claims(event);
    doThrow(new RuntimeException()).when(publisher).publish(event);

    worker.relayBatch();

    assertThat(event.getLastError()).contains("RuntimeException");
  }

  @Test
  void anOversizedErrorMessage_isTruncatedForTheForensicColumn() {
    supportsWithDefaultPolicy();
    OutboxEvent event = pending(1L, 0);
    claims(event);
    doThrow(new RuntimeException("x".repeat(10_000))).when(publisher).publish(event);

    worker.relayBatch();

    assertThat(event.getLastError()).hasSize(OutboxRelayWorker.LAST_ERROR_MAX_LENGTH);
  }
}
