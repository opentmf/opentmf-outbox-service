package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxPublisher.Lane;

/** Lane and key frozen at append; nothing a publisher does here can fail the append. */
class OutboxLaneStamperTests {

  private final OutboxPublisher publisher = mock(OutboxPublisher.class, CALLS_REAL_METHODS);
  private final OutboxLaneStamper stamper =
      new OutboxLaneStamper(new OutboxPublisherRouter(List.of(publisher)));

  private static OutboxEvent row(String destination) {
    OutboxEvent event = new OutboxEvent();
    event.setDestination(destination);
    return event;
  }

  @Test
  void anOrderedRow_getsTheLane_andNoKey_evenIfThePublisherNamesOne() {
    when(publisher.supports(any())).thenReturn(true);
    when(publisher.orderingKey(any())).thenReturn("ignored-on-the-ordered-lane");
    OutboxEvent event = row("topic");

    stamper.stamp(event);

    assertThat(event.getLane()).isEqualTo(Lane.ORDERED);
    assertThat(event.getOrderingKey()).isNull();
  }

  @Test
  void aConcurrentRow_getsTheLaneAndThePublishersKey() {
    when(publisher.supports(any())).thenReturn(true);
    when(publisher.lane(any())).thenReturn(Lane.CONCURRENT);
    when(publisher.orderingKey(any())).thenReturn("hub:sub-1");
    OutboxEvent event = row("https://hub/sub-1");

    stamper.stamp(event);

    assertThat(event.getLane()).isEqualTo(Lane.CONCURRENT);
    assertThat(event.getOrderingKey()).isEqualTo("hub:sub-1");
  }

  @Test
  void aNullLane_fromAPublisher_meansOrdered() {
    when(publisher.supports(any())).thenReturn(true);
    when(publisher.lane(any())).thenReturn(null);
    OutboxEvent event = row("topic");

    stamper.stamp(event);

    assertThat(event.getLane()).isEqualTo(Lane.ORDERED);
  }

  @Test
  void anOverlongKey_isStoredAsItsSha256_equalKeysStayEqual() {
    when(publisher.supports(any())).thenReturn(true);
    when(publisher.lane(any())).thenReturn(Lane.CONCURRENT);
    String longKey = "https://hub.example/" + "x".repeat(300);
    when(publisher.orderingKey(any())).thenReturn(longKey);
    OutboxEvent first = row("a");
    OutboxEvent second = row("b");

    stamper.stamp(first);
    stamper.stamp(second);

    assertThat(first.getOrderingKey())
        .startsWith("sha256:")
        .hasSize("sha256:".length() + 64)
        .isEqualTo(second.getOrderingKey());
    assertThat(OutboxLaneStamper.fit("x".repeat(255))).hasSize(255); // at the width: as is
  }

  @Test
  void anUnroutableRow_isStoredWithoutALane_neverFailingTheAppend() {
    when(publisher.supports(any())).thenReturn(false);
    OutboxEvent event = row("nowhere");

    stamper.stamp(event);

    assertThat(event.getLane()).isNull();
    assertThat(event.getOrderingKey()).isNull();
  }

  @Test
  void aPublisherThrowingWhileNamingItsKey_leavesNoLane() {
    when(publisher.supports(any())).thenReturn(true);
    when(publisher.lane(any())).thenReturn(Lane.CONCURRENT);
    when(publisher.orderingKey(any())).thenThrow(new IllegalStateException("config missing"));
    OutboxEvent event = row("https://hub/x");

    stamper.stamp(event);

    assertThat(event.getLane()).isNull();
    assertThat(event.getOrderingKey()).isNull();
  }

  @Test
  void aBlankKey_isNoKey() {
    assertThat(OutboxLaneStamper.fit(" ")).isNull();
    assertThat(OutboxLaneStamper.fit("")).isNull();
  }
}
