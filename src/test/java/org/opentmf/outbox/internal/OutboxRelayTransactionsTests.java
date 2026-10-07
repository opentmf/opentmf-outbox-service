package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The relay's transactions are READ COMMITTED whatever the application's default: the
 * CONCURRENT claim's re-check must see, in a fresh snapshot, the leases another pod committed
 * before releasing a key's advisory lock.
 */
class OutboxRelayTransactionsTests {

  @Test
  void theRelaysTransactions_areReadCommitted_onTheGivenManager() {
    PlatformTransactionManager manager = mock(PlatformTransactionManager.class);

    TransactionTemplate template = OutboxAutoConfiguration.relayTransactions(manager);

    assertThat(template.getIsolationLevel())
        .isEqualTo(TransactionDefinition.ISOLATION_READ_COMMITTED);
    assertThat(template.getTransactionManager()).isSameAs(manager);
  }
}
