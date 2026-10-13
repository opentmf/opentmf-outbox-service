package org.opentmf.outbox.internal;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * The after-commit nudge: once the business transaction
 * that appended an outbox row COMMITS, poke the relay. {@code fallbackExecution = true} covers
 * non-transactional callers; {@code NOT_SUPPORTED} keeps the listener out of any ambient
 * transaction. Poking never fails the caller - the sweep is the safety net. A row of the ORDERED
 * lane (or of no known lane) also tells the relay to claim that lane on its next pass.
 */
@Slf4j
@RequiredArgsConstructor
class OutboxRelayTrigger {

  private final OutboxRelay relay;

  /** Pokes the relay once the appending transaction has committed. */
  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT, fallbackExecution = true)
  @Transactional(propagation = Propagation.NOT_SUPPORTED)
  public void onAppended(OutboxAppended appended) {
    log.debug("Outbox row {} committed - poking the relay", appended.outboxId());
    if (appended.lane() == Lane.CONCURRENT) {
      relay.poke();
    } else {
      relay.pokeOrdered(); // its claim runs only when ORDERED rows may be due
    }
  }
}
