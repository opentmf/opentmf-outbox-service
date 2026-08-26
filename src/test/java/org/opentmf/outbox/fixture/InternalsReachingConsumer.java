package org.opentmf.outbox.fixture;

import org.opentmf.outbox.internal.OutboxEventRepository;

/** Test fixture ONLY: a consumer reaching past the contract into the internal package. */
interface InternalsReachingConsumer {

  OutboxEventRepository outboxRepository();
}
