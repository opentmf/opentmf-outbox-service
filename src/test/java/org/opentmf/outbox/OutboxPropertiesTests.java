package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/** The one cross-field rule: the ORDERED lease must outlast a Kafka send. */
class OutboxPropertiesTests {

  @Test
  void theDefaults_holdTheRule() {
    OutboxProperties properties = new OutboxProperties();

    assertThat(properties.isOrderedLeaseLongerThanSendTimeout()).isTrue();
    assertThat(properties.getOrdered().getLease()).isEqualTo(Duration.ofSeconds(15));
    assertThat(properties.getLease()).isEqualTo(Duration.ofMinutes(2));
    assertThat(properties.getConcurrent().getMaxInFlight()).isEqualTo(8);
  }

  @Test
  void anOrderedLeaseNotLongerThanTheSendTimeout_breaksTheRule() {
    OutboxProperties properties = new OutboxProperties();
    properties.getOrdered().setLease(Duration.ofSeconds(10)); // == send-timeout

    assertThat(properties.isOrderedLeaseLongerThanSendTimeout()).isFalse();

    properties.getOrdered().setLease(Duration.ofSeconds(5));
    assertThat(properties.isOrderedLeaseLongerThanSendTimeout()).isFalse();
  }

  @Test
  void aMissingValue_isLeftToItsOwnNotNullConstraint() {
    OutboxProperties properties = new OutboxProperties();
    properties.getOrdered().setLease(null);
    assertThat(properties.isOrderedLeaseLongerThanSendTimeout()).isTrue();

    OutboxProperties noTimeout = new OutboxProperties();
    noTimeout.setSendTimeout(null);
    assertThat(noTimeout.isOrderedLeaseLongerThanSendTimeout()).isTrue();
  }
}
