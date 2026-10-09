package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/** The cross-field rules: the ORDERED lease and the Kafka lease must outlast a Kafka send. */
class OutboxPropertiesTests {

  @Test
  void theDefaults_holdTheRule() {
    OutboxProperties properties = new OutboxProperties();

    assertThat(properties.isOrderedLeaseLongerThanSendTimeout()).isTrue();
    assertThat(properties.getOrdered().getLease()).isEqualTo(Duration.ofSeconds(15));
    assertThat(properties.getLease()).isEqualTo(Duration.ofMinutes(2));
    assertThat(properties.getConcurrent().getMaxInFlight()).isEqualTo(8);
    // 1.5.0: the Kafka publisher stays ORDERED unless told otherwise
    assertThat(properties.isKafkaLeaseLongerThanSendTimeout()).isTrue();
    assertThat(properties.getKafka().getLane()).isEqualTo(OutboxPublisher.Lane.ORDERED);
    assertThat(properties.getKafka().getOrderingKey())
        .isEqualTo(OutboxProperties.KafkaOrderingKey.AGGREGATE_ID);
    assertThat(properties.getKafka().getLease()).isEqualTo(Duration.ofSeconds(15));
  }

  @Test
  void aKafkaLeaseNotLongerThanTheSendTimeout_breaksTheRule() {
    OutboxProperties properties = new OutboxProperties();
    properties.getKafka().setLease(Duration.ofSeconds(10)); // == send-timeout

    assertThat(properties.isKafkaLeaseLongerThanSendTimeout()).isFalse();

    properties.getKafka().setLease(null); // left to its own @NotNull
    assertThat(properties.isKafkaLeaseLongerThanSendTimeout()).isTrue();
    properties.getKafka().setLease(Duration.ofSeconds(11));
    properties.setSendTimeout(null);
    assertThat(properties.isKafkaLeaseLongerThanSendTimeout()).isTrue();
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
