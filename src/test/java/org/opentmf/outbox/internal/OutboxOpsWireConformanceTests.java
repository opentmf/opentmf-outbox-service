package org.opentmf.outbox.internal;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OpsFragmentConformance;
import org.opentmf.outbox.OutboxMaintenanceService;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * OUTBOX-OAS-FRAGMENT-SECURITY-RESPONSES-1 (1.7.0): every answer the library's row, bulk-unpark and
 * prune routes give off the happy path is documented by the shipped fragment ON THAT OPERATION, in
 * the media type it is written in - so a verbatim paste survives a black-box suite. Measured under
 * Spring's RFC 9457 rendering: an advice extending {@link ResponseEntityExceptionHandler}, which is
 * what Boot's {@code spring.mvc.problemdetails.enabled} registers. The list routes need the
 * toolkit's resolvers ({@code OutboxOpsProblemDetailsIT}); 401, 403 and 503 are the consumer's
 * security, which the library does not have.
 */
class OutboxOpsWireConformanceTests {

  @ControllerAdvice
  static class ProblemDetails extends ResponseEntityExceptionHandler {}

  private static final String TOO_BIG = "99999999999999999999"; // digits, beyond int64

  private final OutboxMaintenanceService maintenance = mock(OutboxMaintenanceService.class);
  private final MockMvc mvc =
      MockMvcBuilders.standaloneSetup(new OutboxOpsController(maintenance))
          .setControllerAdvice(new ProblemDetails())
          .build();

  @BeforeEach
  void rowsAreMissingOrInTheWrongState() {
    when(maintenance.inspect(anyLong()))
        .thenThrow(new IllegalArgumentException("Outbox row 9 not found"));
    doThrow(new IllegalArgumentException("Outbox row 9 not found")).when(maintenance).unpark(9L);
    doThrow(new IllegalStateException("Outbox row 7 is not parked")).when(maintenance).unpark(7L);
    doThrow(new IllegalArgumentException("Outbox row 9 not found")).when(maintenance).cancel(9L);
    doThrow(new IllegalStateException("Outbox row 7 is relayed")).when(maintenance).cancel(7L);
    when(maintenance.unpark(null, null, null, null))
        .thenThrow(new IllegalArgumentException("An unpark by filter needs a destination"));
  }

  private void answers(String operation, RequestBuilder request, int status) throws Exception {
    OpsFragmentConformance.answers(mvc, operation, request, status);
  }

  @Test
  void anUnknownRow_andAnIdThatIsNoRow_are404s() throws Exception {
    answers("GET /ops/outbox/{id}", get("/ops/outbox/9"), 404);
    answers("POST /ops/outbox/{id}/unpark", post("/ops/outbox/9/unpark"), 404);
    answers("POST /ops/outbox/{id}/cancel", post("/ops/outbox/9/cancel"), 404);
    answers("GET /ops/outbox/{id}", get("/ops/outbox/abc"), 404); // no route takes it
  }

  @Test
  void anIdBeyondInt64_isA400() throws Exception {
    answers("GET /ops/outbox/{id}", get("/ops/outbox/" + TOO_BIG), 400);
    answers("POST /ops/outbox/{id}/unpark", post("/ops/outbox/" + TOO_BIG + "/unpark"), 400);
    answers("POST /ops/outbox/{id}/cancel", post("/ops/outbox/" + TOO_BIG + "/cancel"), 400);
  }

  @Test
  void aRowInTheWrongState_isA409() throws Exception {
    answers("POST /ops/outbox/{id}/unpark", post("/ops/outbox/7/unpark"), 409);
    answers("POST /ops/outbox/{id}/cancel", post("/ops/outbox/7/cancel"), 409);
  }

  @Test
  void theBulkUnpark_refusesAMissingDestination_anUnreadableBody_andAnotherMediaType()
      throws Exception {
    answers(
        "POST /ops/outbox/unpark",
        post("/ops/outbox/unpark").contentType(MediaType.APPLICATION_JSON).content("{}"),
        400);
    answers(
        "POST /ops/outbox/unpark",
        post("/ops/outbox/unpark").contentType(MediaType.APPLICATION_JSON).content("{"),
        400);
    answers(
        "POST /ops/outbox/unpark",
        post("/ops/outbox/unpark").contentType(MediaType.TEXT_PLAIN).content("hub:a"),
        415);
  }

  @Test
  void anAcceptThatAdmitsNoJson_isA406() throws Exception {
    answers("GET /ops/outbox/{id}", get("/ops/outbox/9").accept(MediaType.APPLICATION_XML), 406);
    answers(
        "POST /ops/outbox/maintenance/prune",
        post("/ops/outbox/maintenance/prune").accept(MediaType.TEXT_PLAIN),
        406);
  }

  @Test
  void aMethodThePathDoesNotServe_isA405() throws Exception {
    answers("POST /ops/outbox/unpark", get("/ops/outbox/unpark"), 405);
    answers("GET /ops/outbox/{id}", delete("/ops/outbox/9"), 405);
    answers("POST /ops/outbox/maintenance/prune", get("/ops/outbox/maintenance/prune"), 405);
  }
}
