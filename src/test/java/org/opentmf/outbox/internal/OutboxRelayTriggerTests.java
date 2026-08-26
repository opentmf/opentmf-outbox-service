package org.opentmf.outbox.internal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;

/** The after-commit nudge delegates to the relay. */
class OutboxRelayTriggerTests {

  @Test
  void onAppended_pokesTheRelay() {
    OutboxRelay relay = mock(OutboxRelay.class);
    new OutboxRelayTrigger(relay).onAppended(new OutboxAppended(7L));
    verify(relay).poke();
  }
}
