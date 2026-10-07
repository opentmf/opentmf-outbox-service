package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;

/** The derived {@code inFlight}: a live lease on a row not yet relayed - nothing else. */
class OutboxRowViewTests {

  private static OutboxEvent row(OffsetDateTime claimedUntil, OffsetDateTime relayedOn) {
    OutboxEvent event = new OutboxEvent();
    event.setId(1L);
    event.setClaimedUntil(claimedUntil);
    event.setRelayedOn(relayedOn);
    return event;
  }

  @Test
  void aLiveLeaseOnAPendingRow_isInFlight() {
    OffsetDateTime lease = OffsetDateTime.now().plusMinutes(1);

    OutboxRowView view = OutboxRowView.of(row(lease, null), false);

    assertThat(view.inFlight()).isTrue();
    assertThat(view.claimedUntil()).isEqualTo(lease);
  }

  @Test
  void aLapsedLease_noLease_orARelayedRow_isNotInFlight() {
    OffsetDateTime now = OffsetDateTime.now();
    assertThat(OutboxRowView.of(row(now.minusSeconds(1), null), false).inFlight()).isFalse();
    assertThat(OutboxRowView.of(row(null, null), false).inFlight()).isFalse();
    assertThat(OutboxRowView.of(row(now.plusMinutes(1), now), false).inFlight()).isFalse();
  }
}
