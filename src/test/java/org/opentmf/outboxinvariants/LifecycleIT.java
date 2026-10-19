package org.opentmf.outboxinvariants;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.opentmf.outboxinvariants.Receiver.Delivery;
import org.opentmf.outboxinvariants.Receiver.Script;

/** G - shutdown under load, startup behind a dead pod's leases, two pods on a 100k backlog. */
class LifecycleIT {

  /**
   * G1: a graceful stop of one of two pods under load on both lanes - the stop ends within its
   * three grace stages (relay, ORDERED batch, CONCURRENT sends), nothing is lost, nothing is booked
   * twice, and what the stopped pod did not book is redelivered once by the other.
   */
  @Test
  void g1_gracefulShutdownUnderLoad() {
    try (Scenario s =
        Scenario.start(
            "G1-graceful-shutdown-under-load",
            2,
            "opentmf.outbox.shutdown-grace=2s",
            "opentmf.outbox.lease=5s",
            "opentmf.outbox.ordered.lease=5s",
            "opentmf.outbox.send-timeout=1s")) {
      Receiver.script(
          "inv:g1c/", Script.concurrent().latency(Duration.ofMillis(50), Duration.ofMillis(200)));
      Receiver.script("inv:g1o/", Script.ordered().latency(Duration.ofMillis(50)));
      Scenario.insertConcurrent("inv:g1c/", 250, 4);
      for (int i = 0; i < 100; i++) {
        s.pod(i % 2).append("inv:g1o/q", "g1o-" + i);
      }
      await().atMost(Duration.ofSeconds(20)).until(() -> Receiver.deliveries().size() > 100);
      Pod a = s.pod(0);
      Instant stopping = Instant.now();
      s.stop(a);
      Duration stop = Duration.between(stopping, Instant.now());
      Findings f = s.finish(Duration.ofMinutes(3), Expect.strict().noOrderedIdOrder());
      f.figure(
          "pod-a stopped in %d ms (three stages of a 2 s grace: at most ~6 s)"
              .formatted(stop.toMillis()));
      if (stop.compareTo(Duration.ofSeconds(7)) > 0) {
        f.add(
            "S",
            "the graceful stop took %s - more than its three 2 s grace stages".formatted(stop));
      }
      long sentAfterStop =
          Receiver.deliveries().stream()
              .filter(d -> d.pod().equals("pod-a") && d.start().isAfter(stopping.plusSeconds(7)))
              .count();
      if (sentAfterStop > 0) {
        f.add("S", "the stopped pod started %d sends after its stop".formatted(sentAfterStop));
      }
      f.assertNone();
    }
  }

  /**
   * G2: a pod starts on rows a dead pod leased (claimed_until five seconds ahead) - it waits for
   * the lapse, then takes them over: no send before the lease ends.
   */
  @Test
  void g2_startupBehindADeadPodsLeases_waitsForTheLapse() {
    try (Scenario s = Scenario.start("G2-startup-behind-dead-leases", 0)) {
      Receiver.script("inv:g2/", Script.concurrent());
      s.startPod("pod-a", "opentmf.outbox.sweep-interval=1h"); // migrates; relays only on nudges
      s.pod(0).close();
      Scenario.sql(
          """
          insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
            created_on, attempts, next_attempt_on, lane, ordering_key, claimed_until)
          select 'it', 'g2-' || g, 'it.event.v1', 'inv:g2/k' || g, '{}', now(), 0,
            now() + interval '5 seconds', 'CONCURRENT', 'inv:g2/k' || g, now() + interval '5 seconds'
          from generate_series(1, 20) g\
          """);
      Instant leasedUntil = Instant.now().plusSeconds(5);
      s.startPod("pod-b");
      Findings f = s.finish(Duration.ofSeconds(60), Expect.strict());
      Instant first =
          Receiver.deliveries().stream()
              .map(Delivery::start)
              .min(Comparator.naturalOrder())
              .orElse(Instant.MAX);
      f.figure(
          "first takeover send %d ms after the dead pod's lease end"
              .formatted(Duration.between(leasedUntil, first).toMillis()));
      if (first.isBefore(leasedUntil.minusMillis(200))) {
        f.add(
            "S",
            "a row leased by a dead pod was sent at %s, before its lease ended at %s"
                .formatted(first, leasedUntil));
      }
      f.assertNone();
    }
  }

  /**
   * Whether G3's rows/s bar is asserted ({@code -Dit.absolute-bars}, default true): it is ABSOLUTE,
   * calibrated on the gate's box; a slower host runs with {@code false} and the figure prints.
   */
  private static final boolean ABSOLUTE_BARS =
      Boolean.parseBoolean(System.getProperty("it.absolute-bars", "true"));

  /**
   * G3: two pods start together on a 100,000-row backlog - every row exactly once, throughput over
   * the bar (500 rows/s, see {@link #ABSOLUTE_BARS}), and neither pod starves (each relays at
   * least a quarter).
   */
  @Test
  void g3_twoPodsStartingOnA100kBacklog() {
    try (Scenario s = Scenario.start("G3-two-pods-100k-backlog", 0)) {
      Receiver.script("inv:g3/", Script.concurrent().latency(Duration.ofMillis(1)));
      s.startPod("pod-m", "opentmf.outbox.sweep-interval=1h");
      s.pod(0).close();
      Scenario.insertConcurrent("inv:g3/", 10000, 10);
      String analysed =
          Scenario.sql1(
              "select coalesce(last_analyze, last_autoanalyze)::text || ' reltuples=' || (select"
                  + " reltuples from pg_class where relname = 'outbox') from pg_stat_user_tables"
                  + " where relname = 'outbox'");
      Instant started = Instant.now();
      s.startPod("pod-a");
      s.startPod("pod-b");
      List<String> slow = new CopyOnWriteArrayList<>();
      AtomicBoolean sampling = new AtomicBoolean(true);
      Thread sampler =
          new Thread(
              () -> {
                while (sampling.get()) {
                  String q =
                      Scenario.sql1(
                          "select string_agg(round(extract(epoch from now() - query_start))::text"
                              + " || 's ' || coalesce(wait_event_type, '-') || '/' ||"
                              + " coalesce(wait_event, '-') || ' ' || left(regexp_replace(query,"
                              + " '\\s+', ' ', 'g'), 120), ' || ') from pg_stat_activity where"
                              + " state = 'active' and now() - query_start > interval '3 seconds'"
                              + " and query not like '%pg_stat_activity%'");
                  if (q != null) {
                    slow.add(Instant.now() + " " + q);
                  }
                  try {
                    Waits.interruptibly(Duration.ofSeconds(2));
                  } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                }
              },
              "g3-sampler");
      sampler.setDaemon(true);
      sampler.start();
      try {
        await()
            .pollInterval(Duration.ofSeconds(1))
            .atMost(Duration.ofMinutes(8))
            .until(() -> Scenario.count("relayed_on is null") == 0);
      } finally {
        sampling.set(false);
      }
      Duration took = Duration.between(started, Instant.now());
      Findings f = s.finish(Duration.ofMinutes(1), Expect.strict());
      Map<String, Long> perPod =
          Receiver.deliveries().stream()
              .collect(Collectors.groupingBy(Delivery::pod, Collectors.counting()));
      double rate = 100_000d / Math.max(1, took.toMillis()) * 1000;
      f.figure(
          "100k rows in %d s = %.0f rows/s; per pod %s".formatted(took.toSeconds(), rate, perPod));
      f.figure("the table before the pods started: analysed " + analysed);
      slow.stream().limit(15).forEach(q -> f.figure("statement over 3 s: " + q));
      if (!ABSOLUTE_BARS) {
        f.figure("the bar of 500 rows/s not asserted (-Dit.absolute-bars=false)");
      } else if (rate < 500) {
        f.add(
            "S",
            ("two pods on a 100k backlog relayed %.0f rows/s (bar 500 - absolute, calibrated on the"
                    + " gate's box; on a slower host run with -Dit.absolute-bars=false)")
                .formatted(rate));
      }
      for (String pod : new String[] {"pod-a", "pod-b"}) {
        if (perPod.getOrDefault(pod, 0L) < 25_000) {
          f.add(
              "S",
              "%s relayed only %d of 100k rows - it starved"
                  .formatted(pod, perPod.getOrDefault(pod, 0L)));
        }
      }
      f.assertNone();
    }
  }
}
