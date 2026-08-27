package org.opentmf.outbox.internal;

import com.querydsl.core.types.Predicate;
import java.util.Map;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxMaintenanceService;
import org.opentmf.outbox.OutboxRowView;
import org.opentmf.outbox.OutboxStateFilter;
import org.opentmf.query.tmf630.annotation.Tmf630Response;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.querydsl.binding.QuerydslPredicate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * The library-provided /ops surface: prune (the scheduled-job target), unpark (break-glass),
 * cancel (withdraw an unreleased effect) and the TMF630 list. Consumers own the SECURITY rows
 * - the library documents the expected shape (an admin role on POST+GET /ops/outbox/**) and
 * their security configuration must state it; the endpoints themselves carry no security so
 * the consumer's deny-by-default posture governs. Disable entirely with
 * {@code opentmf.outbox.ops-endpoints=false} for consumers wiring their own surface.
 *
 * <p>ONE wire contract for the estate: the service's {@link IllegalArgumentException} (no such
 * row) answers <b>404</b>, its {@link IllegalStateException} (the row is not in the state the
 * action needs) answers <b>409</b> - mapped here via {@link ResponseStatusException}, no advice
 * bean, so the consumer's own exception handling is untouched.
 */
@RestController
@RequiredArgsConstructor
@RequestMapping("/ops")
public class OutboxOpsController {

  private final OutboxMaintenanceService maintenance;

  /**
   * One retention-pruning pass over terminal (relayed + cancelled) rows. Returns
   * {@code {"outboxRowsPruned": n}}.
   */
  @PostMapping(path = "/outbox/maintenance/prune", produces = MediaType.APPLICATION_JSON_VALUE)
  public Map<String, Long> prune() {
    return Map.of("outboxRowsPruned", maintenance.prune());
  }

  /** Unparks one parked row - parked_on cleared, attempts reset, due now, relay nudged. */
  @PostMapping(path = "/outbox/{id}/unpark", produces = MediaType.APPLICATION_JSON_VALUE)
  public Map<String, Object> unpark(@PathVariable(name = "id") long id) {
    mapped(
        () -> {
          maintenance.unpark(id);
          return null;
        });
    return Map.of("action", "unparked", "id", id);
  }

  /** Cancels one unreleased row - never relayed from now on, retained for audit. */
  @PostMapping(path = "/outbox/{id}/cancel", produces = MediaType.APPLICATION_JSON_VALUE)
  public Map<String, Object> cancel(@PathVariable(name = "id") long id) {
    mapped(
        () -> {
          maintenance.cancel(id);
          return null;
        });
    return Map.of("action", "cancelled", "id", id);
  }

  /**
   * The TMF630 triage list: toolkit predicate over the row fields (aggregateId, eventType,
   * destination, reference, createdOn ranges, and relayed/cancelled/parked via
   * {@code relayedOn} / {@code cancelledOn} / {@code parkedOn} null-filtering - all
   * toolkit-native, so its strict unknown-field validation stays intact). Payloads omitted;
   * use the inspect sibling. The derived-state legs ride the {@code /state/{state}} sibling.
   */
  @Tmf630Response
  @GetMapping(path = "/outbox", produces = MediaType.APPLICATION_JSON_VALUE)
  public Page<OutboxRowView> list(
      @QuerydslPredicate(root = OutboxEvent.class) Predicate predicate, Pageable pageable) {
    return maintenance.list(predicate, null, pageable);
  }

  /**
   * The list narrowed to ONE derived-state leg ({@link OutboxStateFilter} - pending, parked,
   * relayed, cancelled; an unknown value is a 400), with the same toolkit predicate and paging
   * on top. The state rides the PATH, not a query parameter, because tmf630-toolkit's
   * predicate resolver reads the whole parameter map and rejects any non-reserved name
   * before a handler runs.
   *
   * <p>{@code @Tmf630Response} on every list sibling is about RENDERING consistency: without
   * it a {@code Page} serializes as raw {@code PageImpl} JSON ({@code {"content":[...]}})
   * while the plain list renders the toolkit's bare array + count headers - one surface, two
   * wire shapes.
   */
  @Tmf630Response
  @GetMapping(path = "/outbox/state/{state}", produces = MediaType.APPLICATION_JSON_VALUE)
  public Page<OutboxRowView> listByState(
      @PathVariable(name = "state") String state,
      @QuerydslPredicate(root = OutboxEvent.class) Predicate predicate,
      Pageable pageable) {
    OutboxStateFilter filter;
    try {
      filter = OutboxStateFilter.fromWire(state);
    } catch (IllegalArgumentException ex) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
    }
    return maintenance.list(predicate, filter, pageable);
  }

  /** The PARKED rows - the runbook alias of {@code /outbox/state/parked}, one code path. */
  @Tmf630Response
  @GetMapping(path = "/outbox/parked", produces = MediaType.APPLICATION_JSON_VALUE)
  public Page<OutboxRowView> parked(
      @QuerydslPredicate(root = OutboxEvent.class) Predicate predicate, Pageable pageable) {
    return listByState("parked", predicate, pageable);
  }

  /**
   * One row in full (payload + last_error) - the pre-unpark forensic read. The payload rides
   * here by ruling: this surface sits behind the consumer's admin role; the LIST stays
   * metadata-only.
   */
  @GetMapping(path = "/outbox/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
  public OutboxRowView inspect(@PathVariable(name = "id") long id) {
    return mapped(() -> maintenance.inspect(id));
  }

  private static <T> T mapped(Supplier<T> action) {
    try {
      return action.get();
    } catch (IllegalArgumentException ex) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage(), ex);
    } catch (IllegalStateException ex) {
      throw new ResponseStatusException(HttpStatus.CONFLICT, ex.getMessage(), ex);
    }
  }
}
