package org.opentmf.outboxinvariants;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxMaintenanceService;
import org.opentmf.outbox.OutboxRowInFlightException;
import org.opentmf.outbox.OutboxStateFilter;
import org.opentmf.outboxinvariants.Receiver.Script;
import org.opentmf.outboxinvariants.Receiver.Step;
import org.springframework.data.domain.PageRequest;

/**
 * H - the ops surface while the relay works. H2 (the per-lane gauges against the table) is I4,
 * checked at the end of EVERY scenario.
 */
class OpsIT {

  /**
   * H1: with sends in flight, rows pending, parked, relayed and cancelled at once, the list by
   * state adds up to the table (pending + relayed + cancelled - sent-but-cancelled = total; parked
   * within pending), and the inspect of a sending row shows it in flight.
   */
  @Test
  void h1_listByStateAddsUp_andInspectShowsInFlight_underLoad() {
    try (Scenario s = Scenario.start("H1-ops-consistency-under-load", 1)) {
      Receiver.script("inv:h1done/", Script.concurrent());
      Receiver.script(
          "inv:h1park/", Script.concurrent().steps(Step.fail()).budget(1, Duration.ofMillis(50)));
      Receiver.script("inv:h1hold/", Script.concurrent().steps(Step.holdUntilReleased()));
      for (int i = 0; i < 10; i++) {
        s.pod(0).append("inv:h1done/k" + i, "done-" + i);
        s.pod(0).append("inv:h1park/k" + i, "park-" + i);
      }
      await()
          .atMost(Duration.ofSeconds(30))
          .until(
              () ->
                  Scenario.count("relayed_on is not null") == 10
                      && Scenario.count("parked_on is not null") == 10);
      List<Long> holding = new ArrayList<>();
      for (int i = 0; i < 30; i++) {
        holding.add(s.pod(0).append("inv:h1hold/k" + i, "hold-" + i));
      }
      await().atMost(Duration.ofSeconds(10)).until(() -> Receiver.sending().size() == 8);
      List<Long> refused = new ArrayList<>();
      for (long id : holding.subList(25, 30)) {
        if (!Receiver.sending().containsKey(id)) {
          try {
            s.pod(0).maintenance().cancel(id);
          } catch (OutboxRowInFlightException ex) {
            refused.add(id); // claimed into the hand-off queue, not sending
          }
        }
      }
      OutboxMaintenanceService ops = s.pod(0).maintenance();
      long pending = total(ops, OutboxStateFilter.PENDING);
      long parked = total(ops, OutboxStateFilter.PARKED);
      long relayed = total(ops, OutboxStateFilter.RELAYED);
      long cancelled = total(ops, OutboxStateFilter.CANCELLED);
      long all = ops.list(null, null, PageRequest.of(0, 1)).getTotalElements();
      long both = Scenario.count("relayed_on is not null and cancelled_on is not null");
      long sending = Receiver.sending().keySet().iterator().next();
      boolean inFlight = ops.inspect(sending).inFlight();
      long waiting =
          holding.stream()
              .filter(id -> !Receiver.sending().containsKey(id))
              .findFirst()
              .orElseThrow();
      boolean claimedWhenInspected =
          Scenario.count("id = " + waiting + " and claimed_until > now()") == 1;
      boolean waitingInFlight = ops.inspect(waiting).inFlight();
      Receiver.release();
      Findings f = s.finish(Duration.ofSeconds(60), Expect.strict());
      f.figure(
          "pending=%d parked=%d relayed=%d cancelled=%d all=%d"
              .formatted(pending, parked, relayed, cancelled, all));
      if (pending + relayed + cancelled - both != all) {
        f.add(
            "S",
            "the states do not add up: pending %d + relayed %d + cancelled %d - both %d != all %d"
                .formatted(pending, relayed, cancelled, both, all));
      }
      if (parked > pending) {
        f.add("S", "parked %d exceeds pending %d".formatted(parked, pending));
      }
      if (!inFlight) {
        f.add("S", "the inspect of a sending row does not show it in flight");
      }
      f.figure("cancels refused as claimed (queued, decision #233): " + refused);
      if (waitingInFlight != claimedWhenInspected) {
        f.add(
            "S",
            "the inspect of a waiting row says in flight=%s, its lease says claimed=%s"
                .formatted(waitingInFlight, claimedWhenInspected));
      }
      f.assertNone();
    }
  }

  private static long total(OutboxMaintenanceService ops, OutboxStateFilter state) {
    return ops.list(null, state, PageRequest.of(0, 1)).getTotalElements();
  }
}
