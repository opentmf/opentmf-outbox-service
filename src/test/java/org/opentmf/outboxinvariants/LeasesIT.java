package org.opentmf.outboxinvariants;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opentmf.outboxinvariants.Receiver.Delivery;
import org.opentmf.outboxinvariants.Receiver.Script;
import org.opentmf.outboxinvariants.Receiver.Step;

/**
 * B - leases and redelivery: a send that outlives its lease, a pod that dies between its send and
 * its booking, a queued row taken by another pod, a long legitimate send, a skewed clock.
 */
class LeasesIT {

  private static long deliveries(long rowId) {
    return Receiver.deliveries().stream().filter(d -> d.rowId() == rowId).count();
  }

  private static long delivered(long rowId) {
    return Receiver.deliveries().stream().filter(d -> d.rowId() == rowId && d.delivered()).count();
  }

  /**
   * B1: a send hangs past its BOUND (the larger of its 2 s lease and the 1 s send-timeout) in a
   * client that ignores interrupts (1.9.0, ruling 2): the heartbeat renews it up to the bound,
   * interrupts it once, and leaves the lease to lapse; another holder takes the row and books the
   * lapse as a failed attempt (not sent on that claim), the retry delivers; the first holder's late
   * booking is a no-op (its stamp is gone). Two deliveries, attempts 1, one RELAYED booking.
   */
  @Test
  void b1_aHangPastItsBound_isTakenOver_countedOnce_andTheLateBookingIsANoOp() {
    try (Scenario s =
        Scenario.start(
            "B1-hang-past-bound-takeover",
            2,
            "opentmf.outbox.send-timeout=1s",
            "opentmf.outbox.ordered.lease=2s",
            "opentmf.outbox.kafka.lease=2s")) {
      Receiver.script(
          "inv:b1/",
          Script.concurrent()
              .lease(Duration.ofSeconds(2))
              .budget(5, Duration.ofMillis(200))
              .byDelivery()
              .steps(Step.deaf(Duration.ofSeconds(6)), Step.ok()));
      long row = s.pod(0).append("inv:b1/k", "b1");
      Findings f = s.finish(Duration.ofMinutes(1), Expect.strict().mayDuplicate(row));
      f.figure(
          "deliveries=%d delivered=%d, last_error=%s"
              .formatted(
                  deliveries(row),
                  delivered(row),
                  Scenario.sql1("select last_error from outbox where id = " + row)));
      if (delivered(row) != 2) {
        f.add(
            "S",
            "the hung row should be delivered exactly twice (the hang, the retry after the counted lapse), was %d"
                .formatted(delivered(row)));
      }
      if (Scenario.count(
              "id = "
                  + row
                  + " and attempts = 1 and relayed_on is not null and last_error like"
                  + " 'OutboxLeaseLapsedException%'")
          != 1) {
        f.add(
            "S", "the row should be booked relayed once, its lapse counted as one failed attempt");
      }
      f.assertNone();
    }
  }

  /**
   * B2: a pod in its own JVM delivers, then dies (SIGKILL) inside its booking transaction: the
   * booking rolls back, the lease lapses, and another pod redelivers - exactly once more.
   */
  @Test
  void b2_aPodKilledBetweenSendAndBooking_isRedeliveredExactlyOnce() {
    try (Scenario s = Scenario.start("B2-process-death-before-booking", 0)) {
      ChildPod child = s.startChildPod("child", "inv:b2/;lease=PT3S;hookHolds");
      Receiver.script("inv:b2/", Script.concurrent().lease(Duration.ofSeconds(3)));
      Scenario.sql(
          """
          insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
            created_on, attempts, next_attempt_on, lane, ordering_key)
          values ('it', 'b2', 'it.event.v1', 'inv:b2/k', '{}', now(), 0, now(), 'CONCURRENT', 'inv:b2/k')\
          """);
      await().atMost(Duration.ofSeconds(30)).until(() -> !child.booking().isEmpty());
      long row = child.booking().iterator().next();
      s.kill(child); // delivered, booking open: it rolls back with the process
      s.startPod("pod-a");
      Findings f = s.finish(Duration.ofMinutes(1), Expect.strict());
      f.figure("delivered=%d".formatted(delivered(row)));
      if (delivered(row) != 2) {
        f.add(
            "S",
            "the row should be delivered twice - the dead pod's, one redelivery - was %d"
                .formatted(delivered(row)));
      }
      f.assertNone();
    }
  }

  /**
   * B3: a row waits in pod A's hand-off queue (A's one slot held, A's sends measured fast so A
   * claims ahead), its one-second lease lapses there, pod B takes it and sends it; when A's slot
   * frees, A's dequeue finds its stamp gone and does NOT send it - one send in all.
   */
  @Test
  void b3_aQueuedRowWhoseLeaseLapsed_isSentByTheOtherPod_neverByBoth() {
    try (Scenario s =
        Scenario.start(
            "B3-queued-lease-two-pods", 1, "opentmf.outbox.concurrent.max-in-flight=1")) {
      Receiver.script("inv:b3fast/", Script.concurrent());
      Receiver.script("inv:b3hold/", Script.concurrent().steps(Step.holdUntilReleased()));
      Receiver.script("inv:b3short/", Script.concurrent().lease(Duration.ofSeconds(1)));
      for (int i = 0; i < 20; i++) { // A measures fast sends: it claims ahead of its slot
        s.pod(0).append("inv:b3fast/k" + i, "fast-" + i);
      }
      await().atMost(Duration.ofSeconds(20)).until(() -> Receiver.deliveries().size() >= 20);
      long hold = s.pod(0).append("inv:b3hold/k", "hold");
      await().atMost(Duration.ofSeconds(10)).until(() -> Receiver.sending().containsKey(hold));
      long queued = s.pod(0).append("inv:b3short/k", "short");
      await() // A claimed it into its queue
          .atMost(Duration.ofSeconds(10))
          .until(() -> Scenario.count("id = " + queued + " and claimed_until is not null") == 1);
      await().pollDelay(Duration.ofMillis(1500)).atMost(Duration.ofSeconds(3)).until(() -> true);
      Pod b = s.startPod("pod-b");
      await().atMost(Duration.ofSeconds(20)).until(() -> delivered(queued) >= 1);
      Receiver.release(); // A's slot frees: it dequeues the row B already sent
      Findings f = s.finish(Duration.ofMinutes(1), Expect.strict());
      List<String> senders =
          Receiver.deliveries().stream()
              .filter(d -> d.rowId() == queued)
              .map(Delivery::pod)
              .toList();
      f.figure("the queued row was sent by " + senders + " (B is " + b + ")");
      if (delivered(queued) != 1) {
        f.add(
            "S",
            "the queued row should be sent once in all, was %d (%s)"
                .formatted(delivered(queued), senders));
      }
      f.assertNone();
    }
  }

  /**
   * B4: a legitimate long send - three times its lease - on pod A, pod B running: the send is never
   * taken over while A is alive and sending.
   */
  @Test
  void b4_aLongLegitimateSend_isNeverStolenWhileItsPodLives() {
    try (Scenario s = Scenario.start("B4-long-send-3x-lease", 2)) {
      Receiver.script(
          "inv:b4/",
          Script.concurrent()
              .lease(Duration.ofSeconds(2))
              .byDelivery()
              .steps(Step.hang(Duration.ofSeconds(6)), Step.ok()));
      long row = s.pod(0).append("inv:b4/k", "b4");
      Findings f = s.finish(Duration.ofMinutes(1), Expect.strict().mayDuplicate(row));
      f.figure(
          "delivered=%d (%s)"
              .formatted(
                  delivered(row),
                  Receiver.deliveries().stream()
                      .filter(d -> d.rowId() == row)
                      .map(Delivery::pod)
                      .toList()));
      if (delivered(row) != 1) {
        f.add(
            "S",
            "a send of 3x its lease on a live pod was taken over: delivered %d times - no lease renewal while a send runs"
                .formatted(delivered(row)));
      }
      f.assertNone();
    }
  }

  /**
   * B5: clock skew - three pods on one database, one whose clock runs 5 min BEHIND, one 5 min AHEAD
   * ({@code it.clock-skew}, the library's clock built on the skewed one). A row the behind pod
   * sends under its lease is never taken over by the others (its lease must not end already for
   * them), a row another pod sends is never stolen by the ahead pod, and a row the ahead pod
   * appends is relayed at once (its first due time must not lie 5 min ahead). Re-run with {@code
   * -Dit.clock-corrected=false} to see 1.8.0: every pod on its own clock.
   */
  @Test
  void b5_skewedClocks_stealNothing_loseNothing_delayNothing() {
    String corrected = "it.clock-corrected=" + System.getProperty("it.clock-corrected", "true");
    try (Scenario s = Scenario.start("B5-clock-skew", 0)) {
      Receiver.script("inv:b5/", Script.concurrent().steps(Step.holdUntilReleased()));
      Receiver.script("inv:b5ok/", Script.concurrent());
      Pod behind = s.startPod("pod-behind", "it.clock-skew=-5m", corrected);
      long heldByBehind = behind.append("inv:b5/behind", "b5-behind");
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> Receiver.sending().containsKey(heldByBehind));
      Pod normal = s.startPod("pod-a");
      long heldByNormal = normal.append("inv:b5/normal", "b5-normal");
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> Receiver.sending().containsKey(heldByNormal));
      Pod ahead = s.startPod("pod-ahead", "it.clock-skew=5m", corrected);
      long appendedAhead = ahead.append("inv:b5ok/ahead", "b5-ahead");
      pause(Duration.ofSeconds(4)); // four sweeps on every pod
      boolean aheadSent =
          Receiver.deliveries().stream().anyMatch(d -> d.rowId() == appendedAhead && d.delivered());
      Receiver.release();
      Findings end =
          s.finish(
              Duration.ofMinutes(1),
              Expect.strict().mayDuplicate(heldByBehind).mayDuplicate(heldByNormal));
      long normalSends =
          Receiver.deliveries().stream().filter(d -> d.rowId() == heldByNormal).count();
      long behindTotal =
          Receiver.deliveries().stream().filter(d -> d.rowId() == heldByBehind).count();
      end.figure(
          "%s; the behind pod's row sent %d times, the normal pod's %d, the ahead pod's appended row relayed within 4 s: %s"
              .formatted(corrected, behindTotal, normalSends, aheadSent));
      if (behindTotal > 1) {
        end.add(
            "S",
            "a pod 5 min behind stamped a lease that had already lapsed for the other pods: its send was taken over (%d sends)"
                .formatted(behindTotal));
      }
      if (normalSends > 1) {
        end.add(
            "S",
            "a pod whose clock runs 5 min ahead took a row under another pod's live lease (%d sends)"
                .formatted(normalSends));
      }
      if (!aheadSent) {
        end.add(
            "S",
            "a row appended by a pod 5 min ahead was not relayed within 4 s - its first due time"
                + " lay in the future");
      }
      end.assertNone();
    }
  }

  private static void pause(Duration d) {
    await().pollDelay(d).atMost(d.plusSeconds(5)).until(() -> true);
  }
}
