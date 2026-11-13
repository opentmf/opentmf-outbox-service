package org.opentmf.outbox;

import org.springframework.test.context.TestPropertySource;

/** {@link KafkaTraceContinuityIT} on the CONCURRENT lane, a plain producer. */
@TestPropertySource(properties = "opentmf.outbox.kafka.lane=CONCURRENT")
class KafkaTraceContinuityConcurrentIT extends KafkaTraceContinuityIT {}
