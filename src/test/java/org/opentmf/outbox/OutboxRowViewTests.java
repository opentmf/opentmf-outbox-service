package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

/** The derived {@code inFlight}: a live lease on a row not yet relayed - nothing else. */
class OutboxRowViewTests {

  private static OutboxEvent row(OffsetDateTime claimedUntil, OffsetDateTime relayedOn) {
    OutboxEvent event = new OutboxEvent();
    event.setId(1L);
    event.setClaimedUntil(claimedUntil);
    event.setRelayedOn(relayedOn);
    return event;
  }

  @Test
  void aLiveLeaseOnAPendingRow_isInFlight() {
    OffsetDateTime lease = OffsetDateTime.now().plusMinutes(1);

    OutboxRowView view = OutboxRowView.of(row(lease, null), false);

    assertThat(view.inFlight()).isTrue();
    assertThat(view.claimedUntil()).isEqualTo(lease);
  }

  @Test
  void aLapsedLease_noLease_orARelayedRow_isNotInFlight() {
    OffsetDateTime now = OffsetDateTime.now();
    assertThat(OutboxRowView.of(row(now.minusSeconds(1), null), false).inFlight()).isFalse();
    assertThat(OutboxRowView.of(row(null, null), false).inFlight()).isFalse();
    assertThat(OutboxRowView.of(row(now.plusMinutes(1), now), false).inFlight()).isFalse();
  }

  // ---------------------------------------------------- OUTBOX-OPS-PAYLOAD-OWN-PATH-1 (1.11.1)

  private static final OffsetDateTime AT = OffsetDateTime.parse("2026-11-13T10:00:00Z");

  private static OutboxEvent failedRow() {
    OutboxEvent event = new OutboxEvent();
    event.setId(42L);
    event.setAggregateType("subscription");
    event.setAggregateId("sub-1");
    event.setEventType("hub.event.v1");
    event.setDestination("https://hub/cb");
    event.setAttempts(2);
    event.setCreatedOn(AT);
    event.setNextAttemptOn(AT);
    event.setPayload("{\"msisdn\":\"905551112233\"}");
    event.setLastError(
        "HttpServerErrorException.InternalServerError: 500 Internal Server Error on POST request"
            + " for \"https://hub/cb\": {\"echo\":{\"msisdn\":\"905551112233\"}}");
    return event;
  }

  /** The default views as up to 1.11.0, byte for byte. */
  @Test
  void byDefault_theViewIsTodays_byteForByte() {
    String json =
        JsonMapper.builder().build().writeValueAsString(OutboxRowView.of(failedRow(), true, AT));

    assertThat(json)
        .isEqualTo(
            "{\"id\":42,\"aggregateType\":\"subscription\",\"aggregateId\":\"sub-1\","
                + "\"eventType\":\"hub.event.v1\",\"destination\":\"https://hub/cb\","
                + "\"clientProfile\":null,\"reference\":null,\"attempts\":2,\"parked\":false,"
                + "\"inFlight\":false,\"createdOn\":\"2026-11-13T10:00:00Z\","
                + "\"nextAttemptOn\":\"2026-11-13T10:00:00Z\",\"releaseAt\":null,"
                + "\"parkedOn\":null,\"claimedUntil\":null,\"relayedOn\":null,"
                + "\"cancelledOn\":null,"
                + "\"lastError\":\"HttpServerErrorException.InternalServerError:"
                + " 500 Internal Server Error on POST request for \\\"https://hub/cb\\\":"
                + " {\\\"echo\\\":{\\\"msisdn\\\":\\\"905551112233\\\"}}\","
                + "\"payload\":\"{\\\"msisdn\\\":\\\"905551112233\\\"}\"}");
  }

  /**
   * With the sensitive text kept off the views the payload is null and {@code lastError} holds the
   * failure's kind alone - in the record itself, so no {@code fields=} selection, which reads the
   * accessors, brings the text back.
   */
  @Test
  void withTheSensitiveTextOffTheViews_noPayload_andLastErrorIsTheKindOnly() {
    OutboxRowView view = OutboxRowView.of(failedRow(), true, false, AT);

    assertThat(view.payload()).isNull();
    assertThat(view.lastError()).isEqualTo("HttpServerErrorException.InternalServerError");
    assertThat(JsonMapper.builder().build().writeValueAsString(view))
        .doesNotContain("905551112233", "500 Internal Server Error", "https://hub/cb\\\"");
  }

  @Test
  void theKind_isTheLeadingTypeName_neverAPieceOfTheText() {
    assertThat(OutboxRowView.kindOf(null)).isNull();
    assertThat(
            OutboxRowView.kindOf(
                "HttpClientErrorException.BadRequest: 400 Bad Request on POST request for"
                    + " \"https://hub/cb\": {\"msisdn\":\"905551112233\"}"))
        .isEqualTo("HttpClientErrorException.BadRequest"); // the type name, nothing of the body
    assertThat(OutboxRowView.kindOf("KafkaException: Failed to publish outbox row 7 to t"))
        .isEqualTo("KafkaException");
    assertThat(OutboxRowView.kindOf("java.lang.IllegalStateException")) // toString, no message
        .isEqualTo("java.lang.IllegalStateException");
    // a text of the consumer's own, or anything else not led by a type name: the fixed word
    assertThat(OutboxRowView.kindOf("{\"echo\":\"a body\"}")).isEqualTo("unclassified");
    assertThat(OutboxRowView.kindOf("customer 905551112233 refused")).isEqualTo("unclassified");
    assertThat(OutboxRowView.kindOf("905551112233: refused")).isEqualTo("unclassified");
    assertThat(OutboxRowView.kindOf("a..b: two dots")).isEqualTo("unclassified");
    assertThat(OutboxRowView.kindOf("")).isEqualTo("unclassified");
    String longest = "A".repeat(200 - "Exception".length()) + "Exception";
    assertThat(OutboxRowView.kindOf("A" + longest + ": too long a name")).isEqualTo("unclassified");
    assertThat(OutboxRowView.kindOf(longest + ": the longest name")).isEqualTo(longest);
  }

  @Test
  void theKind_ofAWordThatNamesNoThrowable_isUnclassified() {
    String unclassified = OutboxRowView.UNCLASSIFIED;
    Map<String, String> kinds = new LinkedHashMap<>();
    // a type name's shape is not enough: a receiver's word, a name or a dotted name before a colon
    // has it, and the kind is shown to a role that may not read the error (1.11.1)
    kinds.put("boom", unclassified);
    kinds.put("boom: x", unclassified);
    kinds.put("Mueller: not found", unclassified);
    kinds.put("John.Smith: not found", unclassified);
    kinds.put("a.b", unclassified);
    // a consumer's own exception not named ...Exception or ...Error: its text on the payload path
    kinds.put("Timeout: the consumer's own class", unclassified);
    kinds.put("Müller: not found", unclassified);
    kinds.put("4915112345678: rejected", unclassified);
    kinds.put("no colon at all", unclassified);
    // a throwable's name stays; "Error" alone carries no value
    kinds.put("Error: Müller unknown", "Error");
    kinds.put("TimeoutException: t", "TimeoutException");
    kinds.put("java.util.concurrent.TimeoutException: t", "java.util.concurrent.TimeoutException");
    kinds.put(
        "org.springframework.web.client.HttpClientErrorException$BadRequest: 400 x",
        "org.springframework.web.client.HttpClientErrorException$BadRequest");
    kinds.put("BadRequest: 400 Bad Request: \"{}\"", "BadRequest");

    assertSoftly(
        softly ->
            kinds.forEach(
                (text, kind) ->
                    softly.assertThat(OutboxRowView.kindOf(text)).as(text).isEqualTo(kind)));
  }
}
