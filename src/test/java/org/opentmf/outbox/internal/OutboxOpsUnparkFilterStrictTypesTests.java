package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentmf.outbox.OpsFragmentConformance;
import org.opentmf.outbox.OutboxMaintenanceService;
import org.opentmf.outbox.OutboxUnparkResult;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * OUTBOX-UNPARK-FILTER-STRICT-TYPES-1 (1.8.0): the unpark by filter types its JSON body STRICTLY
 * and never coerces. Up to 1.7.0 it bound {@code destination} as a plain String through the
 * consumer's mapper, which turns {@code false} into "false" - a destination that passed the blank
 * check and answered 200. Now a field of the wrong JSON type is a 400 naming the field, whatever
 * the consumer's mapper does, and the service is never called.
 */
class OutboxOpsUnparkFilterStrictTypesTests {

  @ControllerAdvice
  static class ProblemDetails extends ResponseEntityExceptionHandler {}

  private final OutboxMaintenanceService maintenance = mock(OutboxMaintenanceService.class);
  private final MockMvc mvc =
      MockMvcBuilders.standaloneSetup(new OutboxOpsController(maintenance))
          .setControllerAdvice(new ProblemDetails())
          .build();

  OutboxOpsUnparkFilterStrictTypesTests() {
    when(maintenance.unpark(any(), any(), any(), any()))
        .thenReturn(new OutboxUnparkResult(0, false));
  }

  /** Every field of the filter, each with every JSON value of the wrong type. */
  static Stream<Arguments> wrongTypes() {
    List<Arguments> cases = new ArrayList<>();
    for (String field : List.of("destination", "reference", "parkedFrom", "parkedTo")) {
      for (String value : List.of("false", "true", "0", "1", "1.5", "{}", "[]", "[\"hub:a\"]")) {
        cases.add(Arguments.of(field, value));
      }
    }
    return cases.stream();
  }

  private static String body(String field, String value) {
    String destination = "destination".equals(field) ? value : "\"hub:a\"";
    String rest = "destination".equals(field) ? "" : ", \"" + field + "\": " + value;
    return "{\"destination\": " + destination + rest + "}";
  }

  @ParameterizedTest(name = "{0} = {1}")
  @MethodSource("wrongTypes")
  void aFieldOfTheWrongJsonType_isA400_andNothingIsUnparked(String field, String value)
      throws Exception {
    MediaType written =
        OpsFragmentConformance.answers(
            mvc,
            "POST /ops/outbox/unpark",
            post("/ops/outbox/unpark")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body(field, value)),
            400);

    assertThat(written).isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
    verify(maintenance, never()).unpark(any(), any(), any(), any());
  }

  @Test
  void aDateTimeThatIsNotRfc3339_isA400() throws Exception {
    for (String value : List.of("\"yesterday\"", "\"2026-10-16T10:00:00\"", "\"\"")) {
      OpsFragmentConformance.answers(
          mvc,
          "POST /ops/outbox/unpark",
          post("/ops/outbox/unpark")
              .contentType(MediaType.APPLICATION_JSON)
              .content(body("parkedFrom", value)),
          400);
    }
    verify(maintenance, never()).unpark(any(), any(), any(), any());
  }

  @Test
  void aWellTypedFilter_reachesTheService_withEveryFieldAsSent() throws Exception {
    mvc.perform(
            post("/ops/outbox/unpark")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    """
                    {"destination": "hub:a", "parkedFrom": "2026-10-16T10:00:00Z",
                     "parkedTo": "2026-10-16T12:00:00+02:00", "reference": null,
                     "somethingElse": 42}\
                    """))
        .andReturn();

    verify(maintenance)
        .unpark(
            "hub:a",
            OffsetDateTime.parse("2026-10-16T10:00:00Z"),
            OffsetDateTime.parse("2026-10-16T12:00:00+02:00"),
            null);
  }
}
