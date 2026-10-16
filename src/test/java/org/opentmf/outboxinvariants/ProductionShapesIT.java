package org.opentmf.outboxinvariants;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxAppend;
import org.opentmf.outboxinvariants.Receiver.Delivery;
import org.opentmf.outboxinvariants.Receiver.Script;
import org.opentmf.outboxinvariants.Receiver.Step;

/** K - the estate's production shapes, and a seeded randomised soak. */
class ProductionShapesIT {

  private static void pause(Duration d) {
    await().pollDelay(d).atMost(d.plusSeconds(5)).until(() -> true);
  }

  /**
   * K1: THE FLOW SHAPE (dnms-flow's config): archive legs over HTTP of 30-40 s each, keyed {@code
   * http:<dispatchId>}, beside Kafka rows with a 15 s lease, 8 slots, 2 pods, three bursts. Every
   * Kafka row goes within 2 s of a free slot, none twice; the max wait is reported.
   */
  @Test
  void k1_theFlowShape_archiveLegsBesideKafkaRows() {
    try (Scenario s = Scenario.start("K1-flow-shape", 2)) {
      Receiver.script(
          "inv:http/",
          Script.concurrent()
              .latency(Duration.ofSeconds(30), Duration.ofSeconds(40))
              .lease(Duration.ofMinutes(2)));
      Receiver.script(
          "inv:kafka/",
          Script.concurrent()
              .latency(Duration.ofMillis(1), Duration.ofMillis(3))
              .lease(Duration.ofSeconds(15)));
      int dispatch = 0;
      for (int burst = 0; burst < 3; burst++) {
        for (int i = 0; i < 12; i++, dispatch++) {
          s.pod(i % 2).append("inv:http/dispatch-" + dispatch, "d-" + dispatch);
        }
        for (int i = 0; i < 40; i++) {
          s.pod(i % 2).append("inv:kafka/agg-" + (burst * 20 + i % 20), "k-" + burst + "-" + i);
        }
        pause(Duration.ofSeconds(15));
      }
      Findings f =
          s.finish(Duration.ofMinutes(4), Expect.strict().starvationBound(Duration.ofSeconds(2)));
      f.assertNone();
    }
  }

  /**
   * K2: THE ADAPTER SHAPE: an ORDERED (Kafka-like) lane to a slow consumer beside CONCURRENT HTTP
   * callbacks, release_at holds (scheduledSendTime), callbacks parking, and an unpark by filter
   * while the relay runs.
   */
  @Test
  void k2_theAdapterShape_orderedSlowConsumer_callbacks_holds_parksAndUnpark() {
    try (Scenario s = Scenario.start("K2-adapter-shape", 2)) {
      Receiver.script(
          "inv:ko/", Script.ordered().latency(Duration.ofMillis(50), Duration.ofMillis(200)));
      Receiver.script(
          "inv:cb/", Script.concurrent().latency(Duration.ofMillis(100), Duration.ofMillis(500)));
      Receiver.script(
          "inv:cbdown/", Script.concurrent().steps(Step.fail()).budget(2, Duration.ofMillis(200)));
      OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
      for (int i = 0; i < 60; i++) {
        Pod pod = s.pod(i % 2);
        pod.append("inv:ko/q", "ko-" + i);
        if (i % 4 == 0) {
          pod.append("inv:cb/receiver-" + (i % 6), "cb-" + i, now.plusSeconds(2 + i % 7));
        } else {
          pod.append("inv:cb/receiver-" + (i % 6), "cb-" + i);
        }
        if (i % 10 == 0) {
          pod.append("inv:cbdown/receiver", "down-" + i);
        }
      }
      await()
          .atMost(Duration.ofSeconds(60))
          .until(
              () ->
                  Scenario.count("destination = 'inv:cbdown/receiver' and parked_on is not null")
                      == 6);
      Receiver.script("inv:cbdown/", Script.concurrent()); // the receiver is back
      long unparked =
          s.pod(1).maintenance().unpark("inv:cbdown/receiver", null, null, null).unparked();
      Findings f = s.finish(Duration.ofMinutes(3), Expect.strict().attemptsReset());
      f.figure("unparked by filter: " + unparked);
      if (unparked != 6) {
        f.add(
            "S", "the unpark by filter returned %d of the 6 parked callbacks".formatted(unparked));
      }
      f.assertNone();
    }
  }

  /**
   * K3: RANDOMISED - a seeded generator of rows (lane, key, duration, outcome, release_at) and
   * actions (cancel, unpark, a pod stopping and another starting, a database outage) for 90 s on up
   * to three pods; then the six invariants. Re-run a failure with {@code -Dit.seed=<seed>}.
   */
  @Test
  void k3_randomised_seeded() {
    try (Scenario s =
        Scenario.start(
            "K3-randomised",
            2,
            "opentmf.outbox.lease=6s",
            "opentmf.outbox.ordered.lease=6s",
            "opentmf.outbox.send-timeout=2s",
            "opentmf.outbox.backoff-base=300ms",
            "opentmf.outbox.backoff-cap=2s",
            "spring.datasource.hikari.connection-timeout=3000")) {
      Receiver.script(
          "inv:r/o/", Script.ordered().latency(Duration.ofMillis(1), Duration.ofMillis(80)));
      Receiver.script(
          "inv:r/c/", Script.concurrent().latency(Duration.ofMillis(1), Duration.ofMillis(300)));
      Receiver.script(
          "inv:r/flaky/",
          Script.concurrent()
              .byDelivery()
              .steps(Step.fail(), Step.ok())
              .budget(5, Duration.ofMillis(200)));
      Receiver.script(
          "inv:r/dead/", Script.concurrent().steps(Step.fail()).budget(2, Duration.ofMillis(200)));
      Receiver.script(
          "inv:r/slow/",
          Script.concurrent()
              .byDelivery()
              .lease(Duration.ofSeconds(2))
              .steps(Step.hang(Duration.ofSeconds(3)), Step.ok()));
      List<Long> appended = new ArrayList<>();
      long end = System.nanoTime() + Duration.ofSeconds(90).toNanos();
      boolean stopped = false;
      boolean outage = false;
      int n = 0;
      while (System.nanoTime() < end) {
        long elapsed = Duration.ofSeconds(90).toNanos() - (end - System.nanoTime());
        if (!stopped && elapsed > Duration.ofSeconds(30).toNanos()) {
          s.stop(s.pod(0));
          s.startPod("pod-c");
          stopped = true;
        }
        if (!outage && elapsed > Duration.ofSeconds(55).toNanos()) {
          s.outage(Duration.ofSeconds(5));
          outage = true;
        }
        Pod pod =
            s.pods().stream()
                .filter(Pod::alive)
                .skip(Receiver.nextLong(2))
                .findFirst()
                .orElse(s.pods().get(1));
        double kind = Receiver.nextDouble();
        String key = "k" + Receiver.nextLong(25);
        String destination =
            kind < 0.30
                ? "inv:r/o/q"
                : kind < 0.85
                    ? "inv:r/c/" + key
                    : kind < 0.93
                        ? "inv:r/flaky/" + key
                        : kind < 0.97 ? "inv:r/dead/" + key : "inv:r/slow/" + key;
        OutboxAppend row = OutboxAppend.of("it", "r-" + n++, "it.random.v1", destination, Map.of());
        if (Receiver.nextDouble() < 0.10) {
          row =
              row.withReleaseAt(
                  OffsetDateTime.now(ZoneOffset.UTC).plusNanos(Receiver.nextLong(5_000_000_000L)));
        }
        try {
          appended.add(pod.append(row));
        } catch (RuntimeException ex) {
          s.note("append during the outage refused: " + ex.getClass().getSimpleName());
        }
        double action = Receiver.nextDouble();
        if (action < 0.03 && !appended.isEmpty()) {
          long victim = appended.get((int) Receiver.nextLong(appended.size()));
          try {
            pod.maintenance().cancel(victim);
          } catch (RuntimeException ex) {
            // in flight, relayed or already cancelled - all legitimate refusals
          }
        } else if (action < 0.05) {
          try {
            pod.maintenance().unpark("inv:r/dead/" + key, null, null, null);
          } catch (RuntimeException ex) {
            // during the outage
          }
        }
        pause(Duration.ofMillis(20 + Receiver.nextLong(80)));
      }
      Receiver.script(
          "inv:r/dead/", Script.concurrent().steps(Step.fail()).budget(2, Duration.ofMillis(200)));
      Findings f =
          s.finish(Duration.ofMinutes(4), Expect.strict().attemptsReset().noOrderedIdOrder());
      f.figure("appended=%d rows over 90 s".formatted(appended.size()));
      long sent = Receiver.deliveries().stream().filter(Delivery::delivered).count();
      f.figure("delivered=" + sent);
      f.assertNone();
    }
  }
}
