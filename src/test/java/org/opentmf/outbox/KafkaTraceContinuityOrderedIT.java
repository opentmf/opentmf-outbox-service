package org.opentmf.outbox;

import org.springframework.test.context.TestPropertySource;

/** {@link KafkaTraceContinuityIT} on the ORDERED lane, a plain producer. */
@TestPropertySource(properties = "opentmf.outbox.kafka.lane=ORDERED")
class KafkaTraceContinuityOrderedIT extends KafkaTraceContinuityIT {}
