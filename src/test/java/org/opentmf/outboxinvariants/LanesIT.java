package org.opentmf.outboxinvariants;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxPublisher.ExhaustionOutcome;
import org.opentmf.outboxinvariants.Receiver.Delivery;
import org.opentmf.outboxinvariants.Receiver.Script;
import org.opentmf.outboxinvariants.Receiver.Step;

/**
 * A - the lanes and the claim, on two pods over one database. Every scenario ends with the six
 * invariants ({@link Invariants}); the scenario's own expectations are findings "S".
 */
class LanesIT {

  /**
   * A1: CONCURRENT, 40 keys x 3 rows, send durations drawn from 10 ms to 30 s (most short, one in
   * twenty long), 8 slots on each of 2 pods.
   */
  @Test
  void a1_concurrentKeysWithMixedSendDurations_twoPods() {
    try (Scenario s = Scenario.start("A1-concurrent-mixed-durations", 2)) {
      Receiver.script(
          "inv:a1/",
          Script.concurrent()
              .stepsPerRow(
                  e ->
                      List.of(
                          Receiver.nextDouble() < 0.05
                              ? Step.hang(Duration.ofSeconds(2 + Receiver.nextLong(28)))
                              : Step.hang(Duration.ofMillis(10 + Receiver.nextLong(190))))));
      for (int row = 0; row < 3; row++) {
        for (int key = 0; key < 40; key++) {
          s.pod((row + key) % 2).append("inv:a1/k" + key, "a1-" + key + "-" + row);
        }
      }
      s.finish(Duration.ofMinutes(3), Expect.strict()).assertNone();
    }
  }

  /**
   * A2: ORDERED, strict order, one batch in flight per pod; one row hangs for twice its lease. The
   * README's promise: each ORDERED lease covers ONE send; a send that outlives it may be taken by
   * another pod (the documented duplicate of that one row) - the rows behind it go out once, in id
   * order per pod, and never two ORDERED sends at once on one pod.
   */
  @Test
  void a2_orderedStrictOrder_aRowHangingTwiceItsLease() {
    try (Scenario s =
        Scenario.start(
            "A2-ordered-hang-2x-lease",
            2,
            "opentmf.outbox.ordered.lease=3s",
            "opentmf.outbox.send-timeout=1s")) {
      Receiver.script(
          "inv:a2/",
          Script.ordered()
              .latency(Duration.ofMillis(20))
              .byDelivery()
              .stepsPerRow(
                  e ->
                      e.getAggregateId().equals("a2-3")
                          ? List.of(Step.hang(Duration.ofSeconds(6)), Step.ok())
                          : List.of(Step.ok())));
      for (int i = 0; i < 12; i++) {
        s.pod(0).append("inv:a2/q", "a2-" + i);
      }
      Findings f = s.finish(Duration.ofMinutes(2), Expect.strict());
      long hung = Scenario.count("aggregate_id = 'a2-3'");
      f.figure("hung row present=" + hung);
      f.assertNone();
    }
  }

  /**
   * A2b: an ORDERED row whose send ALWAYS outlasts its lease (a publisher slower than its lease -
   * misconfigured, or a receiver that got slower). The lane must not stall: the rows behind it go
   * out, and the slow row ends counted (exhausted) instead of being re-sent every lease forever.
   */
  @Test
  void a2b_anOrderedRowAlwaysSlowerThanItsLease_neverStallsTheLane() {
    try (Scenario s =
        Scenario.start(
            "A2b-ordered-always-slower-than-lease",
            2,
            "opentmf.outbox.ordered.lease=2s",
            "opentmf.outbox.send-timeout=1s")) {
      Receiver.script(
          "inv:a2b/",
          Script.ordered()
              .budget(3, Duration.ofMillis(200))
              .stepsPerRow(
                  e ->
                      e.getAggregateId().equals("a2b-1")
                          ? List.of(Step.hang(Duration.ofSeconds(4)))
                          : List.of(Step.ok())));
      List<Long> rows = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        rows.add(s.pod(0).append("inv:a2b/q", "a2b-" + i));
      }
      Findings f = s.finish(Duration.ofSeconds(60), Expect.strict().noOrderedIdOrder());
      long slow = rows.get(1);
      long slowSends = Receiver.deliveries().stream().filter(d -> d.rowId() == slow).count();
      long behindSent =
          rows.subList(2, rows.size()).stream()
              .filter(
                  id ->
                      Receiver.deliveries().stream()
                          .anyMatch(d -> d.rowId() == id && d.delivered()))
              .count();
      f.figure(
          "the slow row was sent %d times; %d of the 6 rows behind it went"
              .formatted(slowSends, behindSent));
      if (behindSent < 6) {
        f.add(
            "S",
            "the ORDERED lane stalled behind a row slower than its lease: %d of 6 rows behind it sent"
                .formatted(behindSent));
      }
      if (slowSends > 3) {
        f.add(
            "S",
            "the slow row was re-sent %d times - a lapse is never counted, so it never exhausts"
                .formatted(slowSends));
      }
      f.assertNone();
    }
  }

  /**
   * A3: lane independence both ways. (i) a CONCURRENT flood of 10,000 rows: an ORDERED batch
   * appended into it still starts within one relay period; (ii) a CONCURRENT row appended while an
   * ORDERED batch sends (100 ms a row) goes within 2 s.
   */
  @Test
  void a3_aConcurrentFlood_andAnOrderedBatch_neverHoldEachOther() {
    try (Scenario s = Scenario.start("A3-lane-independence-flood", 2)) {
      Receiver.script("inv:flood/", Script.concurrent().latency(Duration.ofMillis(5)));
      Receiver.script("inv:a3o/", Script.ordered().latency(Duration.ofMillis(100)));
      Receiver.script("inv:a3c/", Script.concurrent());
      Scenario.insertConcurrent("inv:flood/", 1000, 10);
      await().atMost(Duration.ofSeconds(20)).until(() -> !Receiver.deliveries().isEmpty());

      Instant orderedAppended = Instant.now();
      List<Long> ordered = new ArrayList<>();
      for (int i = 0; i < 30; i++) {
        ordered.add(s.pod(0).append("inv:a3o/q", "a3o-" + i));
      }
      await().atMost(Duration.ofSeconds(30)).until(() -> deliveredIds().contains(ordered.get(0)));
      Instant firstOrdered = firstStart(ordered.get(0));

      Instant concurrentAppended = Instant.now();
      long urgent = s.pod(1).append("inv:a3c/urgent", "a3c");
      await().atMost(Duration.ofSeconds(60)).until(() -> deliveredIds().contains(urgent));
      Instant urgentSent = firstStart(urgent);

      Findings f = s.finish(Duration.ofMinutes(3), Expect.strict());
      Duration orderedWait = Duration.between(orderedAppended, firstOrdered);
      Duration urgentWait = Duration.between(concurrentAppended, urgentSent);
      f.figure(
          "ORDERED batch's first send %dms after its append, in the flood"
              .formatted(orderedWait.toMillis()));
      f.figure("CONCURRENT row %dms after its append".formatted(urgentWait.toMillis()));
      if (orderedWait.compareTo(Duration.ofSeconds(2)) > 0) {
        f.add(
            "S",
            "the ORDERED batch waited %s behind the CONCURRENT flood (bound: one 1 s sweep + 1 s)"
                .formatted(orderedWait));
      }
      if (urgentWait.compareTo(Duration.ofSeconds(2)) > 0) {
        f.add(
            "S",
            "the CONCURRENT row appended mid-batch waited %s (bound 2 s)".formatted(urgentWait));
      }
      f.assertNone();
    }
  }

  /**
   * A4: keyed CONCURRENT on 2 pods - (i) 20 keys x 5 rows in id order per key; (ii) a key whose
   * receiver fails every time (budget 3, then PARK) blocks no other key; (iii) a key whose first
   * row hangs past its lease: the lease lapses, the row is delivered again, and the key's next row
   * never passes it.
   */
  @Test
  void a4_keyedConcurrent_order_aFailingKey_aHangingKey() {
    try (Scenario s = Scenario.start("A4-keyed-concurrent", 2)) {
      Receiver.script(
          "inv:a4/", Script.concurrent().latency(Duration.ofMillis(5), Duration.ofMillis(50)));
      Receiver.script(
          "inv:a4bad/",
          Script.concurrent()
              .steps(Step.fail())
              .budget(3, Duration.ofMillis(200))
              .exhausted(ExhaustionOutcome.PARK));
      Receiver.script(
          "inv:a4hang/",
          Script.concurrent()
              .lease(Duration.ofSeconds(2))
              .byDelivery()
              .stepsPerRow(
                  e ->
                      e.getAggregateId().equals("h-0")
                          ? List.of(Step.hang(Duration.ofSeconds(5)), Step.ok())
                          : List.of(Step.ok())));
      for (int row = 0; row < 5; row++) {
        s.pod(row % 2).append("inv:a4bad/k", "bad-" + row);
        s.pod(row % 2).append("inv:a4hang/k", "h-" + row);
        for (int key = 0; key < 20; key++) {
          s.pod((row + key) % 2).append("inv:a4/k" + key, "a4-" + key + "-" + row);
        }
      }
      Findings f = s.finish(Duration.ofMinutes(2), Expect.strict());
      long parked = Scenario.count("destination = 'inv:a4bad/k' and parked_on is not null");
      if (parked != 5) {
        f.add("S", "the failing key's 5 rows should all be parked, %d are".formatted(parked));
      }
      f.assertNone();
    }
  }

  /**
   * A4b: a keyed CONCURRENT row whose send ALWAYS outlasts its lease - its key must not stall for
   * good: the slow row ends counted (exhausted) within its budget instead of being re-sent every
   * lease by every pod, and the key's later rows then go.
   */
  @Test
  void a4b_aKeyedRowAlwaysSlowerThanItsLease_neverStallsItsKey() {
    try (Scenario s = Scenario.start("A4b-keyed-always-slower-than-lease", 2)) {
      Receiver.script(
          "inv:a4b/",
          Script.concurrent()
              .lease(Duration.ofSeconds(2))
              .budget(3, Duration.ofMillis(200))
              .stepsPerRow(
                  e ->
                      e.getAggregateId().equals("a4b-0")
                          ? List.of(Step.hang(Duration.ofSeconds(4)))
                          : List.of(Step.ok())));
      List<Long> rows = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        rows.add(s.pod(i % 2).append("inv:a4b/k", "a4b-" + i));
      }
      Findings f = s.finish(Duration.ofSeconds(60), Expect.strict());
      long slow = rows.get(0);
      long slowSends = Receiver.deliveries().stream().filter(d -> d.rowId() == slow).count();
      long behind =
          rows.subList(1, rows.size()).stream()
              .filter(
                  id ->
                      Receiver.deliveries().stream()
                          .anyMatch(d -> d.rowId() == id && d.delivered()))
              .count();
      f.figure(
          "the slow row was sent %d times; %d of the 4 rows behind it on its key went"
              .formatted(slowSends, behind));
      if (behind < 4) {
        f.add(
            "S",
            "the key stalled behind a row slower than its lease: %d of 4 later rows sent"
                .formatted(behind));
      }
      if (slowSends > 3) {
        f.add(
            "S",
            "the slow row was re-sent %d times - a lapse is never counted, so it never exhausts"
                .formatted(slowSends));
      }
      f.assertNone();
    }
  }

  /**
   * A5: round-robin fairness - one hot key with 1,000 rows (serial, by key) against 50 cold keys
   * with one row each: every cold row goes out before the hot key's 20th row.
   */
  @Test
  void a5_roundRobin_coldKeysNeverWaitBehindAHotKey() {
    try (Scenario s = Scenario.start("A5-round-robin-fairness", 1)) {
      Receiver.script("inv:hot/", Script.concurrent().latency(Duration.ofMillis(10)));
      Receiver.script("inv:cold/", Script.concurrent().latency(Duration.ofMillis(10)));
      Scenario.insertConcurrent("inv:hot/", 1, 1000);
      for (int key = 0; key < 50; key++) {
        s.pod(0).append("inv:cold/k" + key, "cold-" + key);
      }
      Findings f = s.finish(Duration.ofMinutes(3), Expect.strict());
      List<Delivery> hot =
          Receiver.deliveries().stream()
              .filter(d -> d.destination().startsWith("inv:hot/"))
              .sorted(Comparator.comparing(Delivery::start))
              .toList();
      Instant lastCold =
          Receiver.deliveries().stream()
              .filter(d -> d.destination().startsWith("inv:cold/"))
              .map(Delivery::start)
              .max(Comparator.naturalOrder())
              .orElse(Instant.MAX);
      long hotBefore = hot.stream().filter(d -> d.start().isBefore(lastCold)).count();
      f.figure("hot rows sent before the last cold row: " + hotBefore);
      if (hotBefore >= 20) {
        f.add("S", "the last cold row went after %d hot rows (bound 20)".formatted(hotBefore));
      }
      f.assertNone();
    }
  }

  static Set<Long> deliveredIds() {
    return Receiver.deliveries().stream().map(Delivery::rowId).collect(Collectors.toSet());
  }

  static Instant firstStart(long rowId) {
    return Receiver.deliveries().stream()
        .filter(d -> d.rowId() == rowId)
        .map(Delivery::start)
        .min(Comparator.naturalOrder())
        .orElseThrow();
  }

  /**
   * A6: a HELD head is stepped over at CLAIM time - and the checker judges the claim, not the send
   * (the gate's K3 finding). One slot, held by a send for 0.7 s (under the lane's one-second live
   * mark, so nothing is given back); row 1 of key k held for 0.4 s; row 2 of key k, due at once, is
   * claimed into the hand-off queue while row 1 is still held, and sent once the slot frees - after
   * row 1's hold has ended. Row 1 then waits for row 2 (its key is in flight) and goes second: the
   * documented pass-over of a held head. The old checker compared the hold with row 2's SEND and
   * flagged it; this scenario provokes exactly that race and requires it.
   */
  @Test
  void a6_aHeldHead_isSteppedOverAtClaimTime_theCheckerJudgesTheClaim() {
    try (Scenario s = Scenario.start("A6-held-head-stepped-over-at-claim", 0)) {
      Receiver.script("inv:a6w/", Script.concurrent()); // a measured fast send: the lane queues
      Receiver.script("inv:a6f/", Script.concurrent().steps(Step.holdUntilReleased())); // the slot
      Receiver.script("inv:a6/", Script.concurrent());
      s.startPod("pod-a", "opentmf.outbox.concurrent.max-in-flight=1");
      Pod pod = s.pod(0);
      long warm = pod.append("inv:a6w/w", "a6-warm");
      await().atMost(Duration.ofSeconds(10)).until(() -> delivered(warm) == 1);
      long slot = pod.append("inv:a6f/x", "a6-slot");
      await().atMost(Duration.ofSeconds(10)).until(() -> Receiver.sending().containsKey(slot));
      Instant slotTaken = Instant.now();
      OffsetDateTime release = OffsetDateTime.now(ZoneOffset.UTC).plusNanos(400_000_000L);
      long held = pod.append("inv:a6/k", "a6-held", release);
      long passer = pod.append("inv:a6/k", "a6-passer");
      await()
          .atMost(Duration.ofSeconds(2))
          .until(() -> Scenario.count("id = " + passer + " and claimed_until is not null") == 1);
      await()
          .pollDelay(Duration.between(Instant.now(), slotTaken.plusMillis(700)).abs())
          .atMost(Duration.ofSeconds(3))
          .until(() -> true);
      Receiver.release(); // the slot frees: the queued row 2 goes
      Findings f = s.finish(Duration.ofMinutes(1), Expect.strict());
      Delivery passerSend = firstDelivered(passer);
      Delivery heldSend = firstDelivered(held);
      Instant passerClaimed =
          Receiver.firstStamp(passer).toInstant().minus(Duration.ofMinutes(2)); // the lease
      Instant hold = release.toInstant();
      String instants =
          "row 2 claimed %s, row 1's hold ended %s, row 2 sent %s, row 1 sent %s"
              .formatted(passerClaimed, hold, passerSend.start(), heldSend.start());
      f.figure(instants);
      System.out.println("INVARIANTS: A6 " + instants); // the evidence, pass or fail
      if (!(passerClaimed.isBefore(hold) && hold.isBefore(passerSend.start()))) {
        f.add(
            "S",
            "the race was not provoked: row 2 must be claimed before row 1's hold ends and sent"
                + " after it");
      }
      if (!heldSend.start().isAfter(passerSend.start())) {
        f.add("S", "the held row went first - no pass-over to judge");
      }
      boolean sendTimeRuleHeld = hold.isAfter(passerSend.start());
      if (sendTimeRuleHeld) {
        f.add(
            "S",
            "the old send-time rule would have accepted it - the scenario no longer pins the"
                + " claim-time rule");
      }
      f.assertNone(); // I2 accepts it: row 1 was held when row 2 was CLAIMED
    }
  }

  private static long delivered(long rowId) {
    return Receiver.deliveries().stream().filter(d -> d.rowId() == rowId && d.delivered()).count();
  }

  private static Delivery firstDelivered(long rowId) {
    return Receiver.deliveries().stream()
        .filter(d -> d.rowId() == rowId && d.delivered())
        .min(Comparator.comparing(Delivery::start))
        .orElseThrow();
  }
}
