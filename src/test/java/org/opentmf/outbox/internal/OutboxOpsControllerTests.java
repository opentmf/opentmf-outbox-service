package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxMaintenanceService;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.web.ErrorResponse;
import org.springframework.web.server.ResponseStatusException;

/**
 * The /ops wire contract pinned INDEPENDENTLY of any consumer exception mapper: the
 * controller's own exceptions are Spring {@link ErrorResponse}s carrying 404 (no such row),
 * 409 (wrong state) and 400 (unknown state leg). A consumer whose global handler swallows
 * {@code ErrorResponse} answers 500 instead - that is the consumer's mapper to fix, and the
 * CHANGELOG names it.
 */
class OutboxOpsControllerTests {

  private final OutboxMaintenanceService maintenance = mock(OutboxMaintenanceService.class);
  private final OutboxOpsController controller = new OutboxOpsController(maintenance);

  private static void assertErrorResponse(Throwable thrown, HttpStatus status) {
    assertThat(thrown)
        .isInstanceOf(ResponseStatusException.class)
        .isInstanceOf(ErrorResponse.class);
    assertThat(((ErrorResponse) thrown).getStatusCode()).isEqualTo(status);
  }

  @Test
  void anUnknownRow_isA404ErrorResponse_onInspectUnparkAndCancel() {
    when(maintenance.inspect(anyLong()))
        .thenThrow(new IllegalArgumentException("Outbox row 9 not found"));
    doThrow(new IllegalArgumentException("Outbox row 9 not found")).when(maintenance).unpark(9L);
    doThrow(new IllegalArgumentException("Outbox row 9 not found")).when(maintenance).cancel(9L);

    assertErrorResponse(catchThrowable(() -> controller.inspect(9L)), HttpStatus.NOT_FOUND);
    assertErrorResponse(catchThrowable(() -> controller.unpark(9L)), HttpStatus.NOT_FOUND);
    assertErrorResponse(catchThrowable(() -> controller.cancel(9L)), HttpStatus.NOT_FOUND);
  }

  @Test
  void aRowInTheWrongState_isA409ErrorResponse_onUnparkAndCancel() {
    IllegalStateException relayed = new IllegalStateException("Outbox row 9 is already relayed");
    doThrow(relayed).when(maintenance).unpark(9L);
    doThrow(relayed).when(maintenance).cancel(9L);

    Throwable unpark = catchThrowable(() -> controller.unpark(9L));
    assertErrorResponse(unpark, HttpStatus.CONFLICT);
    assertThat(unpark).hasMessageContaining("already relayed"); // the reason travels
    assertErrorResponse(catchThrowable(() -> controller.cancel(9L)), HttpStatus.CONFLICT);
  }

  @Test
  void anUnknownStateLeg_isA400ErrorResponse() {
    assertErrorResponse(
        catchThrowable(() -> controller.listByState("dead-lettered", null, PageRequest.of(0, 1))),
        HttpStatus.BAD_REQUEST);
  }

  private static Throwable catchThrowable(Runnable action) {
    try {
      action.run();
    } catch (RuntimeException ex) {
      return ex;
    }
    throw new AssertionError("expected an exception");
  }
}
