package org.opentmf.outbox;

import java.util.List;
import lombok.RequiredArgsConstructor;

/**
 * Routes each claimed row to the first {@link OutboxPublisher} that supports it (bean order -
 * consumers may {@code @Order} their own contributions ahead of the library defaults). A
 * destination no publisher supports is a configuration defect and fails the row loudly into
 * the ordinary backoff-then-park path, where the ops list names it.
 */
@RequiredArgsConstructor
class OutboxPublisherRouter {

  private final List<OutboxPublisher> publishers;

  void publish(OutboxEvent event) {
    for (OutboxPublisher publisher : publishers) {
      if (publisher.supports(event)) {
        publisher.publish(event);
        return;
      }
    }
    throw new IllegalStateException(
        ("No OutboxPublisher supports destination '%s' (row %d) - is the matching starter"
                + " dependency (kafka/http) on the classpath?")
            .formatted(event.getDestination(), event.getId()));
  }
}
