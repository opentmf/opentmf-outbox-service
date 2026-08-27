package org.opentmf.outbox.internal;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxPublisher;

/**
 * Resolves each claimed row to the first {@link OutboxPublisher} that supports it (bean order -
 * consumers may {@code @Order} their own contributions ahead of the library defaults). The
 * worker resolves FIRST, then publishes and books failures with THAT publisher's policy. A
 * destination no publisher supports is a configuration defect and fails the row loudly into
 * the library's backoff-then-park path, where the ops list names it.
 */
@RequiredArgsConstructor
class OutboxPublisherRouter {

  private final List<OutboxPublisher> publishers;

  OutboxPublisher resolve(OutboxEvent event) {
    for (OutboxPublisher publisher : publishers) {
      if (publisher.supports(event)) {
        return publisher;
      }
    }
    throw new IllegalStateException(
        ("No OutboxPublisher supports destination '%s' (row %d) - is the matching starter"
                + " dependency (kafka/http) on the classpath?")
            .formatted(event.getDestination(), event.getId()));
  }
}
