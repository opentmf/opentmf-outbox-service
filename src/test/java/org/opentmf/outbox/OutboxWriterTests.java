package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opentmf.outbox.internal.OutboxAppended;
import org.opentmf.outbox.internal.OutboxClock;
import org.opentmf.outbox.internal.OutboxEventRepository;
import org.opentmf.outbox.internal.OutboxLaneStamper;
import org.opentmf.outbox.internal.OutboxTraceContext;
import org.springframework.context.ApplicationEventPublisher;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/** The append seam: payload frozen at write, headers only when present, the after-insert nudge. */
class OutboxWriterTests {

  private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
  private final ApplicationEventPublisher events = mock(ApplicationEventPublisher.class);
  private final OutboxWriter writer = new OutboxWriter(repository, events, new ObjectMapper());

  private OutboxEvent stubSave() {
    ArgumentCaptor<OutboxEvent> saved = ArgumentCaptor.forClass(OutboxEvent.class);
    when(repository.save(saved.capture()))
        .thenAnswer(
            invocation -> {
              OutboxEvent event = invocation.getArgument(0);
              event.setId(42L);
              return event;
            });
    return null;
  }

  @Test
  void append_freezesTheSerializedPayload_andNudgesAfterInsert() {
    stubSave();

    OutboxEvent saved =
        writer.append("agg", "a-1", "e.v1", "topic", Map.of("k", "v"));

    assertThat(saved.getAggregateType()).isEqualTo("agg");
    assertThat(saved.getAggregateId()).isEqualTo("a-1");
    assertThat(saved.getEventType()).isEqualTo("e.v1");
    assertThat(saved.getDestination()).isEqualTo("topic");
    assertThat(saved.getPayload()).contains("\"k\"");
    assertThat(saved.getHeaders()).isNull(); // no extra headers = null column
    assertThat(saved.getClientProfile()).isNull();
    assertThat(saved.getCreatedOn()).isNotNull();
    assertThat(saved.getNextAttemptOn()).isNotNull();
    assertThat(saved.getReleaseAt()).isNull(); // no hold = deliverable now
    verify(events).publishEvent(new OutboxAppended(42L, null)); // no lane = ORDERED
  }

  @Test
  void append_withHeaders_freezesThem() {
    stubSave();

    OutboxEvent saved =
        writer.append("agg", "a-1", "e.v1", "topic", Map.of(), Map.of("x-schema-version", "1"));

    assertThat(saved.getHeaders()).contains("x-schema-version");
  }

  @Test
  void append_withClientProfile_storesTheSelector() {
    stubSave();

    OutboxEvent saved =
        writer.append(
            "agg", "a-1", "e.v1", "https://hub/cb", "hub-subscriber-7", Map.of(), Map.of());

    assertThat(saved.getClientProfile()).isEqualTo("hub-subscriber-7");
    assertThat(saved.getDestination()).isEqualTo("https://hub/cb");
  }

  @Test
  void append_withReleaseAt_freezesTheHold_andStillNudges() {
    stubSave();
    OffsetDateTime hold = OffsetDateTime.now().plusHours(2);

    OutboxEvent saved =
        writer.append(
            OutboxAppend.of("agg", "a-1", "e.v1", "topic", Map.of("k", "v"))
                .withHeaders(Map.of("x-schema-version", "1"))
                .withReleaseAt(hold)
                .withReference("subscription-42"));

    assertThat(saved.getReleaseAt()).isEqualTo(hold);
    assertThat(saved.getReference()).isEqualTo("subscription-42");
    assertThat(saved.getHeaders()).doesNotContain("subscription-42"); // private, not a header
    assertThat(saved.getHeaders()).contains("x-schema-version");
    assertThat(saved.getClientProfile()).isNull();
    assertThat(saved.getCancelledOn()).isNull();
    // the row is written due-now; eligibility is the claim predicate's job (release_at), so
    // the nudge is harmless - the relay simply finds nothing claimable
    assertThat(saved.getNextAttemptOn()).isBeforeOrEqualTo(OffsetDateTime.now());
    verify(events).publishEvent(new OutboxAppended(42L, null)); // no lane = ORDERED
  }

  @Test
  void append_stampsTheLaneThroughTheLazilyResolvedStamper() {
    stubSave();
    OutboxLaneStamper stamper = mock(OutboxLaneStamper.class);
    doAnswer(
            inv -> {
              inv.<OutboxEvent>getArgument(0).setLane(OutboxPublisher.Lane.CONCURRENT);
              return null;
            })
        .when(stamper)
        .stamp(any());
    OutboxWriter stamping = new OutboxWriter(repository, events, new ObjectMapper(), () -> stamper);

    OutboxEvent saved = stamping.append("agg", "a-1", "e.v1", "https://hub/x", Map.of());

    assertThat(saved.getLane()).isEqualTo(OutboxPublisher.Lane.CONCURRENT);
    // the nudge names the lane: a CONCURRENT row does not arm the ORDERED claim
    verify(events).publishEvent(new OutboxAppended(42L, OutboxPublisher.Lane.CONCURRENT));
  }

  @Test
  void theThreeArgumentWriter_stampsNoLane() {
    stubSave();

    OutboxEvent saved = writer.append("agg", "a-1", "e.v1", "https://hub/x", Map.of());

    assertThat(saved.getLane()).isNull(); // rides ORDERED
  }

  // --- the trace context of the append ---

  private static final String CURRENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

  private OutboxWriter tracing(OutboxTraceContext traceContext) {
    return new OutboxWriter(
        repository, events, new ObjectMapper(), () -> null, OutboxClock.system(),
        () -> traceContext);
  }

  private static Map<String, String> stored(OutboxEvent saved) {
    return new ObjectMapper()
        .readValue(saved.getHeaders(), new TypeReference<Map<String, String>>() {});
  }

  @Test
  void aNonEmptyMapWithoutATraceparentKey_isStamped_andKeepsItsOtherEntries() {
    stubSave();
    OutboxWriter traced =
        tracing(() -> Map.of("traceparent", CURRENT, "tracestate", "vendor=a"));

    OutboxEvent saved =
        traced.append("agg", "a-1", "e.v1", "topic", Map.of(), Map.of("x-schema-version", "1"));

    assertThat(stored(saved))
        .containsOnly(
            Map.entry("x-schema-version", "1"),
            Map.entry("traceparent", CURRENT),
            Map.entry("tracestate", "vendor=a"));
  }

  @Test
  void aTraceparentKeyInAnotherLetterCase_isKept_andNotStampedTwice() {
    stubSave();
    OutboxTraceContext current = mock(OutboxTraceContext.class);
    OutboxWriter traced = tracing(current);

    OutboxEvent saved =
        traced.append(
            OutboxAppend.of("agg", "a-1", "e.v1", "topic", Map.of())
                .withHeaders(Map.of("TraceParent", "00-given-01", "tracestate", "given=1")));

    assertThat(stored(saved))
        .containsOnly(Map.entry("TraceParent", "00-given-01"), Map.entry("tracestate", "given=1"));
    verify(current, never()).current();
  }

  @Test
  void anEmptyMap_isStamped() {
    stubSave();
    OutboxWriter traced = tracing(() -> Map.of("traceparent", CURRENT));

    OutboxEvent saved = traced.append("agg", "a-1", "e.v1", "topic", Map.of(), Map.of());

    assertThat(stored(saved)).containsExactly(Map.entry("traceparent", CURRENT));
  }

  @Test
  void aTracestateTheCallerGives_isNotReplaced() {
    stubSave();
    OutboxWriter traced =
        tracing(() -> Map.of("traceparent", CURRENT, "tracestate", "vendor=a"));

    OutboxEvent saved =
        traced.append("agg", "a-1", "e.v1", "topic", Map.of(), Map.of("TraceState", "given=1"));

    assertThat(stored(saved))
        .containsOnly(Map.entry("TraceState", "given=1"), Map.entry("traceparent", CURRENT));
  }

  @Test
  void noCurrentTraceContext_storesNothing() {
    stubSave();

    OutboxEvent none = tracing(Map::of).append("agg", "a-1", "e.v1", "topic", Map.of());
    OutboxEvent off = tracing(null).append("agg", "a-2", "e.v1", "topic", Map.of());

    assertThat(none.getHeaders()).isNull();
    assertThat(off.getHeaders()).isNull();
  }
}
