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

  /** 1.9.0: the plain HTTP client's read timeout - derived below the send bound, or set below it. */
  @Test
  void theHttpReadTimeout_isDerivedBelowTheSendBound_orSetBelowIt() {
    OutboxProperties properties = new OutboxProperties();
    assertThat(properties.sendBound()).isEqualTo(Duration.ofMinutes(2)); // lease 2 min > 10 s
    assertThat(properties.httpReadTimeout()).isEqualTo(Duration.ofSeconds(108)); // less a tenth
    assertThat(properties.getHttp().getConnectTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(properties.isHttpReadTimeoutWithinTheSendBound()).isTrue();
    assertThat(properties.isHttpConnectTimeoutPositive()).isTrue();

    properties.setLease(Duration.ofSeconds(5));
    properties.setSendTimeout(Duration.ofSeconds(2));
    assertThat(properties.sendBound()).isEqualTo(Duration.ofSeconds(5));
    assertThat(properties.httpReadTimeout()).isEqualTo(Duration.ofSeconds(4)); // at least 1 s off

    properties.setSendTimeout(Duration.ofSeconds(30)); // the bound follows the larger
    assertThat(properties.sendBound()).isEqualTo(Duration.ofSeconds(30));

    properties.getHttp().setReadTimeout(Duration.ofSeconds(29));
    assertThat(properties.httpReadTimeout()).isEqualTo(Duration.ofSeconds(29));
    assertThat(properties.isHttpReadTimeoutWithinTheSendBound()).isTrue();
    properties.getHttp().setReadTimeout(Duration.ofSeconds(30)); // not below the bound
    assertThat(properties.isHttpReadTimeoutWithinTheSendBound()).isFalse();
    properties.getHttp().setReadTimeout(Duration.ZERO);
    assertThat(properties.isHttpReadTimeoutWithinTheSendBound()).isFalse();
    properties.setLease(null); // left to its own @NotNull
    assertThat(properties.isHttpReadTimeoutWithinTheSendBound()).isTrue();

    properties.getHttp().setConnectTimeout(Duration.ZERO);
    assertThat(properties.isHttpConnectTimeoutPositive()).isFalse();
    properties.getHttp().setConnectTimeout(null);
    assertThat(properties.isHttpConnectTimeoutPositive()).isTrue();
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
