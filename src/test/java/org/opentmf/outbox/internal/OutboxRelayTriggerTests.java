package org.opentmf.outbox.internal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxPublisher.Lane;

/**
 * The after-commit nudge delegates to the relay: a CONCURRENT row pokes it, any other row (an
 * ORDERED one, one of no known lane) also arms the ORDERED claim.
 */
class OutboxRelayTriggerTests {

  @Test
  void aConcurrentRow_pokesTheRelay() {
    OutboxRelay relay = mock(OutboxRelay.class);
    new OutboxRelayTrigger(relay).onAppended(new OutboxAppended(7L, Lane.CONCURRENT));
    verify(relay).poke();
    verifyNoMoreInteractions(relay);
  }

  @Test
  void anOrderedRow_orOneOfNoKnownLane_armsTheOrderedClaim() {
    OutboxRelay relay = mock(OutboxRelay.class);
    OutboxRelayTrigger trigger = new OutboxRelayTrigger(relay);
    trigger.onAppended(new OutboxAppended(7L, Lane.ORDERED));
    trigger.onAppended(new OutboxAppended(0L, null));
    verify(relay, times(2)).pokeOrdered();
    verifyNoMoreInteractions(relay);
  }
}
