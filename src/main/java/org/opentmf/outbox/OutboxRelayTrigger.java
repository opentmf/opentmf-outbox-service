package org.opentmf.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * The after-commit nudge (the S14 OrchestrateTrigger pattern): once the business transaction
 * that appended an outbox row COMMITS, poke the relay. {@code fallbackExecution = true} covers
 * non-transactional callers; {@code NOT_SUPPORTED} keeps the listener out of any ambient
 * transaction. Poking never fails the caller - the sweep is the safety net.
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
    relay.poke();
  }
}
