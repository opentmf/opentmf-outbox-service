package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** First supporting publisher wins; an unroutable destination fails LOUD into backoff/park. */
class OutboxPublisherRouterTests {

  private static OutboxEvent eventTo(String destination) {
    OutboxEvent event = new OutboxEvent();
    event.setDestination(destination);
    event.setId(7L);
    return event;
  }

  @Test
  void firstSupportingPublisherWins() {
    AtomicReference<String> delivered = new AtomicReference<>();
    OutboxPublisher http = new OutboxPublisher() {
      @Override public boolean supports(OutboxEvent e) { return e.getDestination().startsWith("http"); }
      @Override public void publish(OutboxEvent e) { delivered.set("http"); }
    };
    OutboxPublisher kafka = new OutboxPublisher() {
      @Override public boolean supports(OutboxEvent e) { return true; }
      @Override public void publish(OutboxEvent e) { delivered.set("kafka"); }
    };

    new OutboxPublisherRouter(List.of(http, kafka)).publish(eventTo("https://hub/cb"));
    assertThat(delivered.get()).isEqualTo("http");

    new OutboxPublisherRouter(List.of(http, kafka)).publish(eventTo("comm.delivery.v1"));
    assertThat(delivered.get()).isEqualTo("kafka");
  }

  @Test
  void noSupportingPublisher_failsLoudWithTheDestinationNamed() {
    assertThatIllegalStateException()
        .isThrownBy(() -> new OutboxPublisherRouter(List.of()).publish(eventTo("comm.x.v1")))
        .withMessageContaining("comm.x.v1");
  }
}
