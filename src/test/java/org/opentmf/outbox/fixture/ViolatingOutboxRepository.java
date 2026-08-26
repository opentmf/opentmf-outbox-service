package org.opentmf.outbox.fixture;

import org.opentmf.outbox.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

/** Test fixture ONLY: the exact misuse the seal rule exists to catch. Never registered. */
interface ViolatingOutboxRepository extends JpaRepository<OutboxEvent, Long> {}
