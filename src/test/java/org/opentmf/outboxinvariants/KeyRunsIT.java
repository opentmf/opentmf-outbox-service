package org.opentmf.outboxinvariants;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opentmf.outbox.OutboxAppend;
import org.opentmf.outbox.OutboxRowInFlightException;
import org.opentmf.outboxinvariants.Receiver.Delivery;
import org.opentmf.outboxinvariants.Receiver.Script;
import org.opentmf.outboxinvariants.Receiver.Step;

/**
 * K - a key's run of consecutive due rows relayed as ONE unit
 * (OUTBOX-A-KEYS-CONSECUTIVE-ROWS-ARE-RELAYED-AS-ONE-UNIT-1). Every cell runs twice: with {@code
 * opentmf.outbox.concurrent.key-run=1} (one row of a key per claim - the relay up to 1.11.0, and
 * an adopter's switch back) and with {@code 8} (the default since 1.12.0: a key's head and up to
 * seven due rows behind it in one claim, sent in order on one slot). K1 is what the run adds;
 * every other cell is a contract of 1.11.0 that the run must
 * keep: id order, a failed or held row ends the run (and may then be passed, as documented), a
 * lapsed lease or a killed pod mid-run, a row appended during a run, a cancel, the claim pause,
 * fairness across keys. The six invariants are checked at the end of every cell, as everywhere.
 */
class KeyRunsIT {

  private static final Duration END = Duration.ofMinutes(1);

  private static String keyRun(int rows) {
    return Scenario.KEY_RUN + "=" + rows;
  }

  private static Path probeDir(String cell, int keyRun) {
    return Path.of("target", "key-runs", cell + "-" + keyRun + "-" + System.nanoTime());
  }

  private static String probe(Path dir) {
    return "it.claim-probe=" + dir.toAbsolutePath();
  }

  /** Every lease a claim wrote (one {@code stampLeases} per lease end), as its row ids. */
  private static List<Set<Long>> stamps(Path dir) {
    try (Stream<Path> files = Files.list(dir)) {
      List<Set<Long>> stamps = new ArrayList<>();
      for (Path file : files.toList()) {
        for (String line : Files.readAllLines(file)) {
          String[] f = line.split(",", 9);
          if (f.length == 9 && f[2].equals("stampLeases")) {
            String given = f[8].substring(0, f[8].indexOf('|')).trim();
            stamps.add(
                given.isEmpty()
                    ? Set.of()
                    : Arrays.stream(given.split(" "))
                        .map(Long::parseLong)
                        .collect(Collectors.toSet()));
          }
        }
      }
      return stamps;
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  /** The claims that leased any of {@code rows}. */
  private static List<Set<Long>> claimsOf(Path dir, List<Long> rows) {
    return stamps(dir).stream().filter(s -> rows.stream().anyMatch(s::contains)).toList();
  }

  private static List<Delivery> deliveriesOf(long row) {
    return Receiver.deliveries().stream()
        .filter(d -> d.rowId() == row)
        .sorted(Comparator.comparing(Delivery::start))
        .toList();
  }

  private static long delivered(long row) {
    return deliveriesOf(row).stream().filter(Delivery::delivered).count();
  }

  private static Optional<Delivery> firstDelivered(long row) {
    return deliveriesOf(row).stream().filter(Delivery::delivered).findFirst();
  }

  private static List<Long> appendRun(Pod pod, String destination, int rows, String tag) {
    List<Long> ids = new ArrayList<>();
    for (int i = 1; i <= rows; i++) {
      ids.add(pod.append(destination, tag + "-r" + i));
    }
    return ids;
  }

  /** The rows' first deliveries go in id order, each after the one before ended. */
  private static void inIdOrderOneAfterAnother(Findings f, List<Long> rows) {
    for (int i = 1; i < rows.size(); i++) {
      Optional<Delivery> before = firstDelivered(rows.get(i - 1));
      Optional<Delivery> after = firstDelivered(rows.get(i));
      if (before.isEmpty() || after.isEmpty()) {
        f.add("K", "row %d or %d was never delivered".formatted(rows.get(i - 1), rows.get(i)));
      } else if (after.get().start().isBefore(before.get().end())) {
        f.add(
            "K",
            "row %d started (%s) before row %d ended (%s)"
                .formatted(rows.get(i), after.get().start(), rows.get(i - 1), before.get().end()));
      }
    }
  }

  /**
   * K1: three rows of one key appended in one go, one pod. With a key run of 8 ONE claim leases
   * all three and they go in id order on one slot; with 1, one claim per row (today).
   */
  @ParameterizedTest(name = "key-run={0}")
  @ValueSource(ints = {1, 8})
  void k1_aKeysRunOfThree_isClaimedOnce_andSentInIdOrder(int keyRun) {
    Path probes = probeDir("K1", keyRun);
    try (Scenario s = Scenario.start("K1-run-of-three-" + keyRun, 1, keyRun(keyRun), probe(probes))) {
      Receiver.script("inv:k1/", Script.concurrent().latency(Duration.ofMillis(50)));
      Pod pod = s.pod(0);
      List<Long> rows = // one business transaction appends the three
          pod.tx()
              .execute(
                  status ->
                      Stream.of("k1-r1", "k1-r2", "k1-r3")
                          .map(id -> pod.writer().append(of(id, "inv:k1/k")).getId())
                          .toList());
      Findings f = s.finish(END, Expect.strict());
      inIdOrderOneAfterAnother(f, rows);
      List<Set<Long>> claims = claimsOf(probes, rows);
      f.figure("claims that leased the run: " + claims);
      int expected = keyRun >= 3 ? 1 : 3;
      if (claims.size() != expected) {
        f.add(
            "K1",
            "key-run=%d: the run of three should take %d claim(s), took %d: %s"
                .formatted(keyRun, expected, claims.size(), claims));
      }
      f.assertNone();
    }
  }

  /**
   * K2: rows 1, 2, 3, 4 of one key; row 2's first send fails. Row 3 is never sent while row 2's
   * failed send runs, nor before it ended - the run ends at the failure. Afterwards row 3 may pass
   * row 2 (a failed row backs off: the documented exception), and it does not wait out a lease.
   */
  @ParameterizedTest(name = "key-run={0}")
  @ValueSource(ints = {1, 8})
  void k2_aFailedMiddleRow_endsTheRun_andNothingBehindItIsSentBeforeItsFailure(int keyRun) {
    try (Scenario s = Scenario.start("K2-failed-middle-" + keyRun, 1, keyRun(keyRun))) {
      Receiver.script(
          "inv:k2/",
          Script.concurrent()
              .latency(Duration.ofMillis(40))
              .budget(5, Duration.ofSeconds(2))
              .stepsPerRow(
                  e ->
                      e.getAggregateId().endsWith("-r2")
                          ? List.of(Step.fail(), Step.ok())
                          : List.of(Step.ok())));
      List<Long> rows = appendRun(s.pod(0), "inv:k2/k", 4, "k2");
      Findings f = s.finish(END, Expect.strict());
      Delivery failed = deliveriesOf(rows.get(1)).get(0);
      for (long behind : rows.subList(2, 4)) {
        Optional<Delivery> first = deliveriesOf(behind).stream().findFirst();
        if (first.isPresent() && first.get().start().isBefore(failed.end())) {
          f.add(
              "K2",
              "row %d was sent (%s) before row 2's failed send ended (%s)"
                  .formatted(behind, first.get().start(), failed.end()));
        }
      }
      inIdOrderOneAfterAnother(f, List.of(rows.get(0), rows.get(1)));
      inIdOrderOneAfterAnother(f, List.of(rows.get(2), rows.get(3)));
      f.figure(
          "row 2 failed at %s, row 3 first sent %s"
              .formatted(failed.end(), deliveriesOf(rows.get(2)).get(0).start()));
      f.assertNone();
    }
  }

  /**
   * K3: rows 1 to 4 of one key, two pods; row 2's first send hangs, deaf, past its bound (lease
   * 2 s, send-timeout 1 s): the heartbeat lets its lease lapse, the other pod counts the lapse as a
   * failed attempt and moves on. The pod whose run lost row 2 sends NOTHING more of that run when
   * the hang returns: rows 3 and 4 are each delivered exactly once, row 1 once, row 2 twice.
   */
  @ParameterizedTest(name = "key-run={0}")
  @ValueSource(ints = {1, 8})
  void k3_aLeaseLapsedMidRun_endsTheRun_andNothingIsSentTwiceOrOutOfTurn(int keyRun) {
    try (Scenario s =
        Scenario.start(
            "K3-lapse-mid-run-" + keyRun,
            2,
            keyRun(keyRun),
            "opentmf.outbox.send-timeout=1s",
            "opentmf.outbox.ordered.lease=2s",
            "opentmf.outbox.kafka.lease=2s")) {
      Receiver.script(
          "inv:k3/",
          Script.concurrent()
              .lease(Duration.ofSeconds(2))
              .latency(Duration.ofMillis(30))
              .budget(5, Duration.ofMillis(200))
              .byDelivery()
              .stepsPerRow(
                  e ->
                      e.getAggregateId().endsWith("-r2")
                          ? List.of(Step.deaf(Duration.ofSeconds(6)), Step.ok())
                          : List.of(Step.ok())));
      List<Long> rows = appendRun(s.pod(0), "inv:k3/k", 4, "k3");
      Findings f = s.finish(END, Expect.strict().mayDuplicate(rows.get(1)));
      long[] expected = {1, 2, 1, 1};
      for (int i = 0; i < 4; i++) {
        if (delivered(rows.get(i)) != expected[i]) {
          f.add(
              "K3",
              "row %d (#%d) should be delivered %d time(s), was %d: %s"
                  .formatted(
                      rows.get(i), i + 1, expected[i], delivered(rows.get(i)),
                      deliveriesOf(rows.get(i))));
        }
      }
      if (Scenario.count(
              "id = "
                  + rows.get(1)
                  + " and attempts = 1 and relayed_on is not null and last_error like"
                  + " 'OutboxLeaseLapsedException%'")
          != 1) {
        f.add("K3", "row 2 should be relayed once, its lapse counted as one failed attempt");
      }
      f.assertNone();
    }
  }

  /**
   * K4: rows 1, 2, 3 of one key on a pod in its own JVM, the booking of row 1 held open; the pod
   * is killed (SIGKILL) inside that booking. Its leases lapse, another pod redelivers row 1 (the
   * at-least-once window a killed pod explains), then rows 2 and 3 in order, each exactly once.
   */
  @ParameterizedTest(name = "key-run={0}")
  @ValueSource(ints = {1, 8})
  void k4_aPodKilledMidRun_isFinishedByAnotherPod_inOrder_andOnlyItsOpenRowTwice(int keyRun) {
    try (Scenario s = Scenario.start("K4-killed-mid-run-" + keyRun, 0, keyRun(keyRun))) {
      ChildPod child = s.startChildPod("child", "inv:k4/;lease=PT3S;latency=PT0.03S;hookHolds");
      Receiver.script(
          "inv:k4/", Script.concurrent().lease(Duration.ofSeconds(3)).latency(Duration.ofMillis(30)));
      List<Long> rows = new ArrayList<>();
      for (int i = 1; i <= 3; i++) {
        Scenario.sql(
            """
            insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
              created_on, attempts, next_attempt_on, lane, ordering_key)
            values ('it', 'k4-r%d', 'it.event.v1', 'inv:k4/k', '{}', now(), 0, now(), 'CONCURRENT',
              'inv:k4/k')\
            """
                .formatted(i));
        rows.add(Long.parseLong(Scenario.sql1("select max(id) from outbox")));
      }
      await().atMost(Duration.ofSeconds(30)).until(() -> !child.booking().isEmpty());
      long open = child.booking().iterator().next();
      s.kill(child); // row 1 delivered, its booking open: it rolls back with the process
      s.startPod("pod-a");
      Findings f = s.finish(END, Expect.strict());
      f.figure("the child was killed in the booking of row %d".formatted(open));
      if (open != rows.get(0)) {
        f.add("K4", "the child booked row %d first, not row %d".formatted(open, rows.get(0)));
      }
      inIdOrderOneAfterAnother(f, rows);
      if (delivered(rows.get(1)) != 1 || delivered(rows.get(2)) != 1) {
        f.add(
            "K4",
            "rows 2 and 3 should be delivered once each, were %d and %d"
                .formatted(delivered(rows.get(1)), delivered(rows.get(2))));
      }
      f.assertNone();
    }
  }

  /**
   * K5: rows 1, 2, 3 of one key, row 2 held until 2 s later. The run ends at the held row: row 3
   * is not leased by the claim that leased row 1. Row 3 may then pass the held row (documented).
   */
  @ParameterizedTest(name = "key-run={0}")
  @ValueSource(ints = {1, 8})
  void k5_aRowNotDueInTheMiddle_endsTheRun(int keyRun) {
    Path probes = probeDir("K5", keyRun);
    try (Scenario s =
        Scenario.start("K5-held-middle-" + keyRun, 1, keyRun(keyRun), probe(probes))) {
      Receiver.script("inv:k5/", Script.concurrent().latency(Duration.ofMillis(30)));
      Pod pod = s.pod(0);
      List<Long> rows =
          pod.tx()
              .execute(
                  status -> {
                    OffsetDateTime later = OffsetDateTime.now().plusSeconds(2);
                    long r1 = pod.writer().append(of("k5-r1", "inv:k5/k")).getId();
                    long r2 =
                        pod.writer().append(of("k5-r2", "inv:k5/k").withReleaseAt(later)).getId();
                    long r3 = pod.writer().append(of("k5-r3", "inv:k5/k")).getId();
                    return List.of(r1, r2, r3);
                  });
      Findings f = s.finish(END, Expect.strict());
      List<Set<Long>> withFirst = claimsOf(probes, List.of(rows.get(0)));
      f.figure("claims that leased row 1: " + withFirst);
      if (withFirst.stream().anyMatch(c -> c.contains(rows.get(2)) || c.contains(rows.get(1)))) {
        f.add("K5", "the claim of row 1 also leased a row behind the held row 2: " + withFirst);
      }
      f.assertNone();
    }
  }

  private static OutboxAppend of(String aggregateId, String destination) {
    return OutboxAppend.of("it", aggregateId, "it.event.v1", destination, Map.of("a", aggregateId));
  }

  /**
   * K6: rows 1, 2, 3 of one key go out from pod A with slow sends; while row 1 is sending, pod B
   * appends row 4. Row 4 is never in flight with a row of the run (I2) and leaves after row 3.
   */
  @ParameterizedTest(name = "key-run={0}")
  @ValueSource(ints = {1, 8})
  void k6_aRowAppendedDuringARun_waitsForTheRun_onEveryPod(int keyRun) {
    try (Scenario s = Scenario.start("K6-appended-during-run-" + keyRun, 2, keyRun(keyRun))) {
      Receiver.script("inv:k6/", Script.concurrent().latency(Duration.ofMillis(300)));
      List<Long> rows = new ArrayList<>(appendRun(s.pod(0), "inv:k6/k", 3, "k6"));
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> Receiver.sending().containsKey(rows.get(0)));
      rows.add(s.pod(1).append("inv:k6/k", "k6-r4"));
      Findings f = s.finish(END, Expect.strict());
      inIdOrderOneAfterAnother(f, rows);
      f.assertNone();
    }
  }

  /**
   * K7: rows 1, 2, 3 of one key, row 1's send held; row 3 is cancelled meanwhile. Either the
   * cancel is refused (the row is leased: in flight) and row 3 is delivered once, after row 2, or
   * it is accepted and row 3 is never sent - never both, never a send after an accepted cancel.
   */
  @ParameterizedTest(name = "key-run={0}")
  @ValueSource(ints = {1, 8})
  void k7_aCancelOfARowInARun_isRefusedOrHonoured_neverHalfOfEach(int keyRun) {
    try (Scenario s = Scenario.start("K7-cancel-in-run-" + keyRun, 1, keyRun(keyRun))) {
      Receiver.script(
          "inv:k7/",
          Script.concurrent()
              .stepsPerRow(
                  e ->
                      e.getAggregateId().endsWith("-r1")
                          ? List.of(Step.holdUntilReleased())
                          : List.of(Step.ok())));
      List<Long> rows = appendRun(s.pod(0), "inv:k7/k", 3, "k7");
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> Receiver.sending().containsKey(rows.get(0)));
      boolean refused = false;
      try {
        s.pod(0).maintenance().cancel(rows.get(2));
      } catch (OutboxRowInFlightException ex) {
        refused = true;
      }
      Receiver.release();
      Expect expect = Expect.strict();
      Findings f = s.finish(END, expect);
      f.figure("key-run=%d: the cancel of row 3 was %s".formatted(keyRun, refused ? "refused" : "accepted"));
      if (refused) {
        inIdOrderOneAfterAnother(f, rows);
        if (delivered(rows.get(2)) != 1) {
          f.add("K7", "the cancel was refused, but row 3 was delivered %d times".formatted(delivered(rows.get(2))));
        }
      } else if (!deliveriesOf(rows.get(2)).isEmpty()) {
        f.add("K7", "row 3 was cancelled and still sent: " + deliveriesOf(rows.get(2)));
      }
      f.assertNone();
    }
  }

  /**
   * K8: a transport paused on the pod (two failed sends over it in a row): its claims take one
   * fresh PROBE row at a time - never a run. A key of four fresh rows over the paused transport:
   * the claim that leased its probe row leased no other row of the key.
   */
  @ParameterizedTest(name = "key-run={0}")
  @ValueSource(ints = {1, 8})
  void k8_whileATransportIsPaused_itsProbeIsOneRow_neverARun(int keyRun) {
    Path probes = probeDir("K8", keyRun);
    try (Scenario s =
        Scenario.start(
            "K8-pause-probe-" + keyRun,
            1,
            keyRun(keyRun),
            probe(probes),
            "opentmf.outbox.claim-pause.after-failures=2",
            "opentmf.outbox.claim-pause.max=3s",
            "opentmf.outbox.backoff-base=500ms")) {
      Receiver.script(
          "inv:k8down/",
          Script.concurrent()
              .transport("t8")
              .budget(2, Duration.ofSeconds(30))
              .steps(Step.fail()));
      Receiver.script("inv:k8/", Script.concurrent().transport("t8").latency(Duration.ofMillis(20)));
      long down1 = s.pod(0).append("inv:k8down/a", "k8-down-a");
      long down2 = s.pod(0).append("inv:k8down/b", "k8-down-b");
      await()
          .atMost(Duration.ofSeconds(20))
          .until(() -> deliveriesOf(down1).size() + deliveriesOf(down2).size() >= 2);
      List<Long> rows = appendRun(s.pod(0), "inv:k8/k", 4, "k8");
      Findings f =
          s.finish(END, Expect.strict().mayStayOpen(down1).mayStayOpen(down2).skipStarvation());
      Instant paused = deliveriesOf(down2).isEmpty() ? null : deliveriesOf(down2).get(0).end();
      Optional<Delivery> probe =
          rows.stream()
              .map(KeyRunsIT::firstDelivered)
              .flatMap(Optional::stream)
              .min(Comparator.comparing(Delivery::start));
      f.figure("paused at %s, first row of the key sent %s".formatted(paused, probe.map(Delivery::start).orElse(null)));
      if (probe.isPresent()) {
        List<Set<Long>> probeClaims = claimsOf(probes, List.of(probe.get().rowId()));
        f.figure("claims that leased the probe: " + probeClaims);
        if (probeClaims.stream().anyMatch(c -> c.stream().filter(rows::contains).count() > 1)) {
          f.add("K8", "the claim of the probe row leased more rows of its key: " + probeClaims);
        }
      } else {
        f.add("K8", "no row of the key was ever sent");
      }
      f.assertNone();
    }
  }

  /**
   * K9: one slot; key A has 40 rows, keys B to E one each, appended after A's. Runs never starve
   * other keys: every one of B to E is first delivered before A's 20th row.
   */
  @ParameterizedTest(name = "key-run={0}")
  @ValueSource(ints = {1, 8})
  void k9_runsAreBounded_andOtherKeysGetTheirTurn(int keyRun) {
    try (Scenario s =
        Scenario.start(
            "K9-fairness-" + keyRun, 1, keyRun(keyRun), "opentmf.outbox.concurrent.max-in-flight=1")) {
      Receiver.script("inv:k9/", Script.concurrent().latency(Duration.ofMillis(10)));
      Scenario.insertConcurrent("inv:k9/a", 1, 40);
      List<Long> others = new ArrayList<>();
      for (String key : List.of("b", "c", "d", "e")) {
        others.add(s.pod(0).append("inv:k9/" + key, "k9-" + key));
      }
      Findings f = s.finish(END, Expect.strict());
      List<Delivery> a =
          Receiver.deliveries().stream()
              .filter(d -> d.key() != null && d.key().equals("inv:k9/a1"))
              .sorted(Comparator.comparing(Delivery::start))
              .toList();
      Instant twentieth = a.size() >= 20 ? a.get(19).start() : Instant.MAX;
      for (long other : others) {
        Optional<Delivery> first = firstDelivered(other);
        if (first.isEmpty() || !first.get().start().isBefore(twentieth)) {
          f.add(
              "K9",
              "row %d of another key waited past key A's 20th row (%s): %s"
                  .formatted(other, twentieth, first.map(Delivery::start).orElse(null)));
        }
      }
      f.assertNone();
    }
  }
}
