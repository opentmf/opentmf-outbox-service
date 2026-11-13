package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** The cross-field rules: the ORDERED lease and the Kafka lease must outlast a Kafka send. */
class OutboxPropertiesTests {

  private static final Validator VALIDATOR =
      Validation.buildDefaultValidatorFactory().getValidator();

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

  /**
   * 1.9.0: the plain HTTP client's read timeout - derived below the send bound, or set below it;
   * 1.11.1: the derived one leaves room for the connect, whose default scales with a short bound.
   */
  @Test
  void theHttpReadTimeout_isDerivedBelowTheSendBound_orSetBelowIt() {
    OutboxProperties properties = new OutboxProperties();
    assertThat(properties.sendBound()).isEqualTo(Duration.ofMinutes(2)); // lease 2 min > 10 s
    assertThat(properties.getHttp().getConnectTimeout()).isNull(); // unset: derived
    assertThat(properties.httpConnectTimeout()).isEqualTo(Duration.ofSeconds(10));
    // less the connect and a tenth: 120 - 10 - 12 (108 s up to 1.11.0)
    assertThat(properties.httpReadTimeout()).isEqualTo(Duration.ofSeconds(98));
    assertThat(properties.isHttpReadTimeoutWithinTheSendBound()).isTrue();
    assertThat(properties.isHttpConnectTimeoutPositive()).isTrue();
    assertThat(properties.getHttpTimeoutsBeyondTheSendBound()).isNull();

    properties.setLease(Duration.ofSeconds(5));
    properties.setSendTimeout(Duration.ofSeconds(2));
    assertThat(properties.sendBound()).isEqualTo(Duration.ofSeconds(5));
    assertThat(properties.httpConnectTimeout()).isEqualTo(Duration.ofMillis(1250)); // a quarter
    assertThat(properties.httpReadTimeout()).isEqualTo(Duration.ofMillis(2750)); // at least 1 s off
    assertThat(properties.getHttpTimeoutsBeyondTheSendBound()).isNull();

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
    assertThat(properties.getHttpTimeoutsBeyondTheSendBound()).as("its own rule").isNull();
    properties.getHttp().setConnectTimeout(null);
    assertThat(properties.isHttpConnectTimeoutPositive()).isTrue();
  }

  /**
   * OUTBOX-HTTP-CONNECT-TIMEOUT-WITHIN-SEND-BOUND-1: the plain client's connect and read end before
   * the send bound together, on every shape that booted on the defaults up to 1.11.0 - the
   * library's own scenarios run leases of 3 to 6 s. Up to 1.11.0 a 30 s lease gave connect 10 s +
   * read 27 s = 37 s.
   */
  @ParameterizedTest(name = "lease {0}, send-timeout {1}")
  @CsvSource({"PT2M, PT10S", "PT30S, PT10S", "PT6S, PT2S", "PT5S, PT2S", "PT3S, PT1S"})
  void onTheDefaults_theConnectAndTheReadEndBeforeTheBound_andEveryShapeBoots(
      Duration lease, Duration sendTimeout) {
    OutboxProperties properties = new OutboxProperties();
    properties.setLease(lease);
    properties.setSendTimeout(sendTimeout);

    assertThat(properties.httpConnectTimeout().plus(properties.httpReadTimeout()))
        .isLessThan(properties.sendBound());
    assertThat(properties.httpReadTimeout()).isPositive();
    assertThat(VALIDATOR.validate(properties)).as("it boots").isEmpty();
  }

  /** What the consumer SET and cannot fit is refused at boot, the figures in the rejected value. */
  @Test
  void setTimeoutsThatCannotEndBeforeTheBound_areRefused() {
    OutboxProperties properties = new OutboxProperties();
    properties.setLease(Duration.ofSeconds(30));
    properties.getHttp().setConnectTimeout(Duration.ofSeconds(10));
    properties.getHttp().setReadTimeout(Duration.ofSeconds(25)); // below the bound: 1.11.0 took it

    assertThat(properties.getHttpTimeoutsBeyondTheSendBound())
        .isEqualTo("connect PT10S + read PT25S is not below the send bound PT30S");
    assertThat(VALIDATOR.validate(properties))
        .singleElement()
        .satisfies(
            violation ->
                assertThat(violation.getMessage())
                    .contains(
                        "opentmf.outbox.http.connect-timeout",
                        "opentmf.outbox.http.read-timeout",
                        "opentmf.outbox.lease",
                        "opentmf.outbox.send-timeout"));

    properties.getHttp().setReadTimeout(null); // the derived read: no room left after 30 s
    properties.getHttp().setConnectTimeout(Duration.ofSeconds(30));
    assertThat(properties.getHttpTimeoutsBeyondTheSendBound())
        .isEqualTo("connect PT30S + read PT-3S is not below the send bound PT30S");

    properties.getHttp().setConnectTimeout(null); // a set read that leaves the connect no room
    properties.getHttp().setReadTimeout(Duration.ofSeconds(29));
    assertThat(properties.getHttpTimeoutsBeyondTheSendBound())
        .isEqualTo("connect PT7.5S + read PT29S is not below the send bound PT30S");

    properties.getHttp().setReadTimeout(Duration.ofSeconds(20)); // 7.5 + 20 < 30
    assertThat(properties.getHttpTimeoutsBeyondTheSendBound()).isNull();
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
