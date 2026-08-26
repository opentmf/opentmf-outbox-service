package org.opentmf.outbox;

import com.querydsl.core.types.Predicate;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opentmf.query.tmf630.annotation.Tmf630Response;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.querydsl.binding.QuerydslPredicate;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The library-provided /ops TRIO (2026-08-26 ruling): prune (the S25.5 CronJob+kicker target),
 * unpark (break-glass) and the TMF630 list. Consumers own the SECURITY rows - the library
 * documents the expected shape (dnms-admin on POST /ops/maintenance/**, POST+GET
 * /ops/outbox/**) and their config-security must state it; the endpoints themselves carry no
 * security so the consumer's deny-by-default posture governs. Disable entirely with
 * {@code opentmf.outbox.ops-endpoints=false} for consumers wiring their own surface.
 */
@RestController
@RequiredArgsConstructor
@RequestMapping("/ops")
public class OutboxOpsController {

  private final OutboxMaintenanceService maintenance;

  /** One retention-pruning pass over relayed rows. Returns {@code {"outboxRowsPruned": n}}. */
  @PostMapping(path = "/outbox/maintenance/prune", produces = MediaType.APPLICATION_JSON_VALUE)
  public Map<String, Long> prune() {
    return Map.of("outboxRowsPruned", maintenance.pruneRelayed());
  }

  /** Unparks one parked row - attempts reset, due now, relay nudged on commit. */
  @PostMapping(path = "/outbox/{id}/unpark", produces = MediaType.APPLICATION_JSON_VALUE)
  public Map<String, Object> unpark(@PathVariable(name = "id") long id) {
    maintenance.unpark(id);
    return Map.of("action", "unparked", "id", id);
  }

  /**
   * The TMF630 triage list: toolkit predicate over the row fields (aggregateId, eventType,
   * destination, createdOn ranges, and pending/relayed via {@code relayedOn} null-filtering -
   * all toolkit-native, so its strict unknown-field validation stays intact). The one state
   * the toolkit CANNOT express - {@code parked}, derived against the CONFIGURED max-attempts -
   * has the dedicated sub-resource below. Payloads omitted; use the inspect sibling.
   */
  @Tmf630Response
  @GetMapping(path = "/outbox", produces = MediaType.APPLICATION_JSON_VALUE)
  public Page<OutboxRowView> list(
      @QuerydslPredicate(root = OutboxEvent.class) Predicate predicate, Pageable pageable) {
    return maintenance.list(predicate, null, pageable);
  }

  /**
   * The PARKED rows - the derived state a wire filter cannot express (pending AND
   * {@code attempts >= max-attempts}, a CONFIG comparison). Paged; the break-glass
   * {@code unpark} takes the ids this names.
   *
   * <p>{@code @Tmf630Response} here is about RENDERING consistency, not filtering: without it
   * a {@code Page} serializes as raw {@code PageImpl} JSON ({@code {"content":[...]}}) while
   * the list sibling renders the toolkit's bare array + count headers - one surface, two wire
   * shapes (caught by the adapter adoption's OpsIT, 2026-08-26).
   */
  @Tmf630Response
  @GetMapping(path = "/outbox/parked", produces = MediaType.APPLICATION_JSON_VALUE)
  public Page<OutboxRowView> parked(Pageable pageable) {
    return maintenance.list(null, OutboxStateFilter.PARKED, pageable);
  }

  /** One row in full (payload + last_error) - the pre-unpark forensic read. */
  @GetMapping(path = "/outbox/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
  public OutboxRowView inspect(@PathVariable(name = "id") long id) {
    return maintenance.inspect(id);
  }
}
