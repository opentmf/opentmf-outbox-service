package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opentmf.outbox.internal.OutboxAppended;
import org.opentmf.outbox.internal.OutboxEventRepository;
import org.springframework.context.ApplicationEventPublisher;
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
    verify(events).publishEvent(new OutboxAppended(42L));
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
                .withReleaseAt(hold));

    assertThat(saved.getReleaseAt()).isEqualTo(hold);
    assertThat(saved.getHeaders()).contains("x-schema-version");
    assertThat(saved.getClientProfile()).isNull();
    assertThat(saved.getCancelledOn()).isNull();
    // the row is written due-now; eligibility is the claim predicate's job (release_at), so
    // the nudge is harmless - the relay simply finds nothing claimable
    assertThat(saved.getNextAttemptOn()).isBeforeOrEqualTo(OffsetDateTime.now());
    verify(events).publishEvent(new OutboxAppended(42L));
  }
}
