package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.internal.OutboxRelayWorker.Leased;
import org.springframework.transaction.support.TransactionOperations;

/**
 * The send heartbeat (1.9.0): a running send's lease is renewed once a third of it has run, in one
 * guarded statement, up to the send's bound - then the send is interrupted, once, and its lease
 * left to lapse.
 */
class OutboxSendHeartbeatTests {

  private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
  private final OutboxPublisher publisher = mock(OutboxPublisher.class, CALLS_REAL_METHODS);
  private final OutboxSendHeartbeat heartbeat =
      new OutboxSendHeartbeat(
          repository,
          TransactionOperations.withoutTransaction(),
          OutboxClock.system(),
          Duration.ofSeconds(10));

  private static OffsetDateTime at(Instant instant) {
    return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
  }

  /** A row leased {@code lease} long, {@code left} of it still to run. */
  private Leased row(long id, Duration lease, Duration left) {
    OutboxEvent event = new OutboxEvent();
    event.setId(id);
    event.setDestination("topic-" + id);
    Instant now = Instant.now();
    return new Leased(event, publisher, at(now.plus(left).minus(lease)), at(now.plus(left)), false);
  }

  @Test
  void aSendAThirdIntoItsLease_isRenewedToALeaseFromNow_andMarked() {
    Leased row = row(7L, Duration.ofSeconds(3), Duration.ofSeconds(1));
    OffsetDateTime before = row.stamp;
    when(repository.renewSending(any(), any(), any(), any())).thenReturn(List.of(7L));
    heartbeat.started(row);

    heartbeat.beat();

    ArgumentCaptor<String> ids = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> stamps = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> renewed = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> since = ArgumentCaptor.forClass(String.class);
    verify(repository)
        .renewSending(ids.capture(), stamps.capture(), renewed.capture(), since.capture());
    assertThat(ids.getValue()).isEqualTo("{7}");
    assertThat(stamps.getValue())
        .matches("\\{\"\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{6}Z\"}");
    assertThat(row.stamp).isAfter(before.plusSeconds(1)); // now + 3 s, from now + 1 s
    assertThat(row.marked).isTrue();
    assertThat(row.lost).isFalse();
    heartbeat.finished(row);
  }

  @Test
  void aSendWithTwoThirdsOfItsLeaseToRun_isLeftAlone() {
    Leased row = row(7L, Duration.ofSeconds(30), Duration.ofSeconds(29));
    heartbeat.started(row);

    heartbeat.beat();

    verify(repository, never()).renewSending(any(), any(), any(), any());
    heartbeat.finished(row);
  }

  @Test
  void aFinishedSend_isNeverRenewed() {
    Leased row = row(7L, Duration.ofSeconds(3), Duration.ofSeconds(1));
    heartbeat.started(row);
    heartbeat.finished(row);

    heartbeat.beat();

    verify(repository, never()).renewSending(any(), any(), any(), any());
  }

  @Test
  void aRowTheStatementDidNotRenew_isLost() {
    Leased row = row(7L, Duration.ofSeconds(3), Duration.ofSeconds(1));
    OffsetDateTime before = row.stamp;
    when(repository.renewSending(any(), any(), any(), any())).thenReturn(List.of());
    heartbeat.started(row);

    heartbeat.beat();

    assertThat(row.lost).isTrue(); // another holder took it, or a booking cleared it
    assertThat(row.stamp).isEqualTo(before);
    heartbeat.finished(row);
  }

  @Test
  void aFailedRenewal_leavesTheStamp_andTheNextTickTriesAgain() {
    Leased row = row(7L, Duration.ofSeconds(3), Duration.ofSeconds(1));
    OffsetDateTime before = row.stamp;
    when(repository.renewSending(any(), any(), any(), any()))
        .thenThrow(new IllegalStateException("database gone"))
        .thenReturn(List.of(7L));
    heartbeat.started(row);

    heartbeat.beat();
    assertThat(row.stamp).isEqualTo(before);
    assertThat(row.lost).isFalse();

    heartbeat.beat();
    assertThat(row.stamp).isAfter(before);
    heartbeat.finished(row);
  }

  @Test
  void aSendPastItsBound_isInterruptedOnce_andRenewedNoMore() throws Exception {
    Leased row = row(7L, Duration.ofSeconds(3), Duration.ofSeconds(1));
    CountDownLatch registered = new CountDownLatch(1);
    CountDownLatch interrupted = new CountDownLatch(1);
    AtomicBoolean flagClearedAfter = new AtomicBoolean();
    Thread sender =
        new Thread(
            () -> {
              heartbeat.started(row);
              registered.countDown();
              try {
                new CountDownLatch(1).await(10, TimeUnit.SECONDS); // "hangs"
              } catch (InterruptedException ex) {
                interrupted.countDown();
                Thread.currentThread().interrupt(); // as a client would leave it
              } finally {
                heartbeat.finished(row);
                flagClearedAfter.set(!Thread.currentThread().isInterrupted());
              }
            });
    sender.start();
    assertThat(registered.await(5, TimeUnit.SECONDS)).isTrue();
    row.sendStartedAt = Instant.now().minusSeconds(11); // past max(3 s, 10 s)

    heartbeat.beat();
    heartbeat.beat();

    assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
    sender.join(5000);
    assertThat(flagClearedAfter).isTrue(); // the booking does not inherit the interrupt
    assertThat(row.interrupted).isTrue();
    verify(repository, never()).renewSending(any(), any(), any(), any());
  }

  @Test
  void theBound_isTheLargerOfTheLeaseAndTheSendTimeout_thePublishersOwnFirst() {
    Leased shortLease = row(1L, Duration.ofSeconds(3), Duration.ofSeconds(3));
    Leased longLease = row(2L, Duration.ofSeconds(30), Duration.ofSeconds(30));
    assertThat(heartbeat.bound(shortLease)).isEqualTo(Duration.ofSeconds(10)); // send-timeout
    assertThat(heartbeat.bound(longLease)).isEqualTo(Duration.ofSeconds(30)); // its lease

    doReturn(Duration.ofMinutes(3)).when(publisher).sendTimeout(any()); // Kafka's own verdict
    assertThat(heartbeat.bound(shortLease)).isEqualTo(Duration.ofMinutes(3));
    assertThat(heartbeat.bound(longLease)).isEqualTo(Duration.ofMinutes(3));

    doReturn(Duration.ZERO).when(publisher).sendTimeout(any()); // nonsense: not declared
    assertThat(heartbeat.bound(shortLease)).isEqualTo(Duration.ofSeconds(10));
  }

  @Test
  void startAndStop_runTheTickerThread() throws Exception {
    Leased row = row(7L, Duration.ofSeconds(3), Duration.ofSeconds(1));
    CountDownLatch renewed = new CountDownLatch(1);
    when(repository.renewSending(any(), any(), any(), any()))
        .thenAnswer(
            inv -> {
              renewed.countDown();
              return List.of(7L);
            });
    heartbeat.started(row);
    heartbeat.start();
    try {
      assertThat(renewed.await(5, TimeUnit.SECONDS)).isTrue();
    } finally {
      heartbeat.stop();
      heartbeat.finished(row);
    }
  }
}
