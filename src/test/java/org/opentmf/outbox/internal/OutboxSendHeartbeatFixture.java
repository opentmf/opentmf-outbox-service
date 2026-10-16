package org.opentmf.outbox.internal;

import static org.mockito.Mockito.mock;

import java.time.Duration;
import org.springframework.transaction.support.TransactionOperations;

/** A send heartbeat for tests that are not about it: it tracks sends and renews nothing. */
final class OutboxSendHeartbeatFixture {

  private OutboxSendHeartbeatFixture() {}

  /** Never started: no tick, no renewal - the 1.8.0 relay as far as leases go. */
  static OutboxSendHeartbeat idle() {
    return new OutboxSendHeartbeat(
        mock(OutboxEventRepository.class),
        TransactionOperations.withoutTransaction(),
        OutboxClock.system(),
        Duration.ofSeconds(10));
  }
}
