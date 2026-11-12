package org.opentmf.outbox;

import org.springframework.test.context.TestPropertySource;

/** {@link KafkaTraceContinuityIT} on the CONCURRENT lane, a transactional producer. */
@TestPropertySource(
    properties = {
      "opentmf.outbox.kafka.lane=CONCURRENT",
      "spring.kafka.producer.transaction-id-prefix=it-trace-concurrent-"
    })
class KafkaTraceContinuityConcurrentTransactionalIT extends KafkaTraceContinuityIT {}
