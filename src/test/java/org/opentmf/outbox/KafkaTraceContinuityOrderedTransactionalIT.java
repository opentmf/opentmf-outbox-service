package org.opentmf.outbox;

import org.springframework.test.context.TestPropertySource;

/** {@link KafkaTraceContinuityIT} on the ORDERED lane, a transactional producer. */
@TestPropertySource(
    properties = {
      "opentmf.outbox.kafka.lane=ORDERED",
      "spring.kafka.producer.transaction-id-prefix=it-trace-ordered-"
    })
class KafkaTraceContinuityOrderedTransactionalIT extends KafkaTraceContinuityIT {}
