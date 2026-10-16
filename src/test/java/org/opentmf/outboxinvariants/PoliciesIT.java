package org.opentmf.outboxinvariants;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxPublisher.ExhaustionOutcome;
import org.opentmf.outboxinvariants.Receiver.Delivery;
import org.opentmf.outboxinvariants.Receiver.Script;
import org.opentmf.outboxinvariants.Receiver.Step;

/** C - the per-publisher policy, park and unpark, cancel, the release_at hold, poison rows. */
class PoliciesIT {

  private static List<Delivery> of(long rowId) {
    return Receiver.deliveries().stream()
        .filter(d -> d.rowId() == rowId)
        .sorted(Comparator.comparing(Delivery::start))
        .toList();
  }

  /**
   * C1: retry - exactly the configured attempts, with the backoff between them; park - parked_on
   * stamped and never claimed again under two pods and a 10,000-row flood; drop - booked relayed
   * with the forensics, the dropped counter moves, and no relayed listener fires.
   */
  @Test
  void c1_retryParkAndDrop_atItLevel() {
    try (Scenario s = Scenario.start("C1-policy-retry-park-drop", 2)) {
      Receiver.script(
          "inv:c1retry/",
          Script.concurrent()
              .steps(Step.fail(), Step.fail(), Step.ok())
              .budget(5, Duration.ofMillis(400)));
      Receiver.script(
          "inv:c1park/", Script.concurrent().steps(Step.fail()).budget(3, Duration.ofMillis(200)));
      Receiver.script(
          "inv:c1drop/",
          Script.concurrent()
              .steps(Step.fail())
              .budget(2, Duration.ofMillis(200))
              .exhausted(ExhaustionOutcome.DROP));
      Receiver.script("inv:flood/", Script.concurrent().latency(Duration.ofMillis(2)));
      long retry = s.pod(0).append("inv:c1retry/k", "retry");
      long park = s.pod(1).append("inv:c1park/k", "park");
      long drop = s.pod(0).append("inv:c1drop/k", "drop");
      await()
          .atMost(Duration.ofSeconds(30))
          .until(() -> Scenario.count("id = " + park + " and parked_on is not null") == 1);
      Scenario.insertConcurrent("inv:flood/", 1000, 10);
      Findings f = s.finish(Duration.ofMinutes(3), Expect.strict());
      List<Delivery> retries = of(retry);
      if (retries.size() != 3) {
        f.add(
            "S",
            "the retried row should be delivered 3 times (fail, fail, ok), was %d"
                .formatted(retries.size()));
      }
      for (int i = 1; i < retries.size(); i++) {
        Duration gap = Duration.between(retries.get(i - 1).end(), retries.get(i).start());
        if (gap.compareTo(Duration.ofMillis(350)) < 0) {
          f.add(
              "S",
              "retry %d came %dms after the failure, inside the 400 ms backoff"
                  .formatted(i + 1, gap.toMillis()));
        }
      }
      if (of(park).size() != 3) {
        f.add(
            "S",
            "the parked row was delivered %d times - 3 attempts, then never again"
                .formatted(of(park).size()));
      }
      if (of(drop).size() != 2
          || Scenario.count("id = " + drop + " and relayed_on is not null and parked_on is null")
              != 1) {
        f.add(
            "S",
            "the dropped row: %d attempts (2 expected), relayed_on stamped expected"
                .formatted(of(drop).size()));
      }
      if (Receiver.listened().containsKey(drop)) {
        f.add("S", "a relayed listener fired for the DROPPED row");
      }
      f.assertNone();
    }
  }

  /**
   * C2: (i) unpark by id and by filter at once, while two relays claim: every parked row is sent
   * exactly once after; (ii) unpark and cancel of one parked row at once: one of them wins - the
   * row is either sent once or cancelled and never sent after its cancel.
   */
  @Test
  void c2_unparkByIdAndByFilterAtOnce_andUnparkVsCancel() throws Exception {
    try (Scenario s = Scenario.start("C2-unpark-races", 2)) {
      Receiver.script(
          "inv:c2/",
          Script.concurrent()
              .byDelivery()
              .steps(Step.fail(), Step.ok())
              .budget(1, Duration.ofMillis(100)));
      List<Long> parked = new ArrayList<>();
      for (int i = 0; i < 30; i++) {
        parked.add(s.pod(i % 2).append("inv:c2/k" + i, "c2-" + i));
      }
      await()
          .atMost(Duration.ofSeconds(30))
          .until(
              () -> Scenario.count("destination like 'inv:c2/%' and parked_on is not null") == 30);
      long raced = parked.get(29);
      CountDownLatch go = new CountDownLatch(1);
      List<CompletableFuture<String>> calls = new ArrayList<>();
      for (long id : parked.subList(0, 29)) {
        calls.add(
            CompletableFuture.supplyAsync(
                () -> attempt(go, () -> s.pod(0).maintenance().unpark(id))));
      }
      calls.add(
          CompletableFuture.supplyAsync(
              () ->
                  attempt(go, () -> s.pod(1).maintenance().unpark("inv:c2/k0", null, null, null))));
      CompletableFuture<String> unpark =
          CompletableFuture.supplyAsync(
              () -> attempt(go, () -> s.pod(0).maintenance().unpark(raced)));
      CompletableFuture<String> cancel =
          CompletableFuture.supplyAsync(
              () -> attempt(go, () -> s.pod(1).maintenance().cancel(raced)));
      go.countDown();
      String unparkOutcome = unpark.get(30, TimeUnit.SECONDS);
      String cancelOutcome = cancel.get(30, TimeUnit.SECONDS);
      for (CompletableFuture<String> call : calls) {
        call.get(30, TimeUnit.SECONDS);
      }
      Findings f = s.finish(Duration.ofMinutes(2), Expect.strict().attemptsReset());
      f.figure("unpark vs cancel: unpark=" + unparkOutcome + " cancel=" + cancelOutcome);
      for (long id : parked.subList(0, 29)) {
        long sends = of(id).stream().filter(Delivery::delivered).count();
        if (sends != 1) {
          f.add(
              "S",
              "unparked row %d delivered %d times after two unparks at once (1 expected)"
                  .formatted(id, sends));
        }
      }
      long racedSends = of(raced).stream().filter(Delivery::delivered).count();
      boolean cancelled = Scenario.count("id = " + raced + " and cancelled_on is not null") == 1;
      if (racedSends > 1
          || (!cancelled && racedSends != 1)
          || (cancelled
              && racedSends != 0
              && Scenario.count("id = " + raced + " and relayed_on is not null") == 0)) {
        f.add(
            "S",
            "unpark vs cancel left row %d sent %d times, cancelled=%s"
                .formatted(raced, racedSends, cancelled));
      }
      f.assertNone();
    }
  }

  private static String attempt(CountDownLatch go, Supplier<?> call) {
    try {
      go.await();
      call.get();
      return "ok";
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      return "interrupted";
    } catch (RuntimeException ex) {
      return ex.getClass().getSimpleName();
    }
  }

  private static String attempt(CountDownLatch go, Runnable call) {
    return attempt(
        go,
        () -> {
          call.run();
          return null;
        });
  }

  /**
   * C3: cancel of a QUEUED row (claimed, waiting for a slot, not sending) and of a row whose send
   * is in flight - both CLAIMED, both refused by name (409; decision #233: the database cannot tell
   * queued from sending, "in flight" = claimed), the refusal saying so; both then book, and a
   * cancel after the booking answers "already relayed".
   */
  @Test
  void c3_cancelAQueuedRow_andAnInFlightRow() {
    try (Scenario s =
        Scenario.start(
            "C3-cancel-queued-and-in-flight", 1, "opentmf.outbox.concurrent.max-in-flight=1")) {
      Receiver.script("inv:c3fast/", Script.concurrent());
      Receiver.script("inv:c3hold/", Script.concurrent().steps(Step.holdUntilReleased()));
      Receiver.script("inv:c3q/", Script.concurrent());
      for (int i = 0; i < 20; i++) {
        s.pod(0).append("inv:c3fast/k" + i, "fast-" + i);
      }
      await().atMost(Duration.ofSeconds(20)).until(() -> Receiver.deliveries().size() >= 20);
      long holding = s.pod(0).append("inv:c3hold/k", "hold");
      await().atMost(Duration.ofSeconds(10)).until(() -> Receiver.sending().containsKey(holding));
      long queued = s.pod(0).append("inv:c3q/k", "queued");
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> Scenario.count("id = " + queued + " and claimed_until is not null") == 1);
      String cancelQueued = refusal(() -> s.pod(0).maintenance().cancel(queued));
      String cancelInFlight = refusal(() -> s.pod(0).maintenance().cancel(holding));
      Receiver.release();
      Findings f = s.finish(Duration.ofMinutes(1), Expect.strict());
      String afterBooking = refusal(() -> s.pod(0).maintenance().cancel(queued));
      f.figure(
          "cancel of the queued row: "
              + cancelQueued
              + "; of the in-flight row: "
              + cancelInFlight
              + "; after the booking: "
              + afterBooking);
      for (String refused : List.of(cancelQueued, cancelInFlight)) {
        if (!refused.startsWith("OutboxRowInFlightException")
            || !refused.contains("claimed by a relay")) {
          f.add("S", "a cancel of a claimed row was not refused as claimed: " + refused);
        }
      }
      if (!afterBooking.contains("already relayed")) {
        f.add("S", "a cancel after the booking did not answer already relayed: " + afterBooking);
      }
      if (of(queued).stream().noneMatch(Delivery::delivered)
          || of(holding).stream().noneMatch(Delivery::delivered)) {
        f.add("S", "a claimed row whose cancel was refused was not delivered");
      }
      f.assertNone();
    }
  }

  /**
   * C4: the release_at hold under two pods and a flood - never before, within about one sweep
   * after; a release_at in the past is immediate; a held row cancelled before its release is never
   * sent.
   */
  @Test
  void c4_releaseAt_neverBefore_withinAPeriodAfter() {
    try (Scenario s = Scenario.start("C4-release-at", 2)) {
      Receiver.script("inv:c4c/", Script.concurrent());
      Receiver.script("inv:c4o/", Script.ordered());
      Receiver.script("inv:flood/", Script.concurrent().latency(Duration.ofMillis(2)));
      Scenario.insertConcurrent("inv:flood/", 500, 4);
      OffsetDateTime base = OffsetDateTime.now(ZoneOffset.UTC);
      List<Long> held = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        held.add(s.pod(i % 2).append("inv:c4c/k" + i, "c4c-" + i, base.plusSeconds(3 + i % 3)));
        held.add(s.pod(i % 2).append("inv:c4o/q", "c4o-" + i, base.plusSeconds(3 + i % 3)));
      }
      long past = s.pod(0).append("inv:c4c/past", "past", base.minusMinutes(5));
      long cancelled = s.pod(1).append("inv:c4c/cancel", "cancel", base.plusSeconds(4));
      Instant appendedPast = Instant.now();
      s.pod(0).maintenance().cancel(cancelled);
      Findings f = s.finish(Duration.ofMinutes(2), Expect.strict());
      Duration worst = Duration.ZERO;
      for (long id : held) {
        List<Delivery> sends = of(id);
        if (sends.isEmpty()) {
          continue;
        }
        OffsetDateTime releaseAt = base.plusSeconds(3 + (held.indexOf(id) / 2) % 3);
        Duration late = Duration.between(releaseAt.toInstant(), sends.get(0).start());
        worst = late.compareTo(worst) > 0 ? late : worst;
      }
      f.figure("latest send after its release_at: %dms".formatted(worst.toMillis()));
      if (worst.compareTo(Duration.ofMillis(2500)) > 0) {
        f.add(
            "S",
            "a held row went %s after its release_at (bound: one 1 s sweep + 1.5 s)"
                .formatted(worst));
      }
      List<Delivery> pastSends = of(past);
      if (pastSends.isEmpty()
          || Duration.between(appendedPast, pastSends.get(0).start())
                  .compareTo(Duration.ofSeconds(3))
              > 0) {
        f.add("S", "a release_at in the past was not sent at once");
      }
      if (!of(cancelled).isEmpty()) {
        f.add("S", "a held row cancelled before its release was sent");
      }
      f.assertNone();
    }
  }

  /**
   * C5: poison rows - a publisher that throws an Error, a booking hook that throws, a relayed
   * listener that throws, on both lanes, beside healthy rows: each poison row ends counted and
   * parked within its budget, the healthy rows behind it go, and every thread lives (I5).
   */
  @Test
  void c5_poisonRows_areCountedAndParked_theRelayLives() {
    try (Scenario s =
        Scenario.start(
            "C5-poison-rows",
            2,
            "opentmf.outbox.lease=3s",
            "opentmf.outbox.ordered.lease=3s",
            "opentmf.outbox.send-timeout=1s")) {
      Receiver.script(
          "inv:c5err/", Script.concurrent().steps(Step.error()).budget(3, Duration.ofMillis(100)));
      Receiver.script(
          "inv:c5erro/", Script.ordered().steps(Step.error()).budget(3, Duration.ofMillis(100)));
      Receiver.script("inv:c5okc/", Script.concurrent());
      Receiver.script("inv:c5oko/", Script.ordered());
      Receiver.script(
          "inv:c5hook/", Script.concurrent().hookThrows().budget(3, Duration.ofMillis(100)));
      Receiver.script(
          "inv:c5listen/", Script.concurrent().listenerThrows().budget(3, Duration.ofMillis(100)));
      long errC = s.pod(0).append("inv:c5err/k", "err-c");
      long errO = s.pod(0).append("inv:c5erro/q", "err-o");
      List<Long> healthy = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        healthy.add(s.pod(i % 2).append("inv:c5oko/q", "ok-o-" + i));
        healthy.add(s.pod(i % 2).append("inv:c5okc/k" + i, "ok-c-" + i));
      }
      long hook = s.pod(1).append("inv:c5hook/k", "hook");
      long listen = s.pod(1).append("inv:c5listen/k", "listen");
      // a refused booking redelivers (documented): the hook's and the listener's rows go 3 times
      Findings f =
          s.finish(
              Duration.ofSeconds(90),
              Expect.strict().mayDuplicate(listen).mayDuplicate(hook).noOrderedIdOrder());
      for (long poison : List.of(errC, errO, hook, listen)) {
        f.figure(
            "poison row %d: %d deliveries, parked=%s"
                .formatted(
                    poison,
                    of(poison).size(),
                    Scenario.count("id = " + poison + " and parked_on is not null") == 1));
        if (Scenario.count("id = " + poison + " and parked_on is not null") != 1) {
          f.add(
              "S",
              "poison row %d (%s) not parked within its budget of 3: %d deliveries"
                  .formatted(
                      poison,
                      of(poison).isEmpty() ? "-" : of(poison).get(0).destination(),
                      of(poison).size()));
        }
      }
      long stuck =
          healthy.stream()
              .filter(id -> Scenario.count("id = " + id + " and relayed_on is null") == 1)
              .count();
      if (stuck > 0) {
        f.add("S", "%d healthy rows behind the poison rows were never relayed".formatted(stuck));
      }
      f.assertNone();
    }
  }

  /** "ok", or the refusal as {@code SimpleName: message}. */
  private static String refusal(Runnable action) {
    try {
      action.run();
      return "ok";
    } catch (RuntimeException ex) {
      return ex.getClass().getSimpleName() + ": " + ex.getMessage();
    }
  }
}
