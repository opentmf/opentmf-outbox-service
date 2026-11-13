package org.opentmf.outboxinvariants;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import io.micrometer.core.instrument.Gauge;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxAppend;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.opentmf.outboxinvariants.Receiver.Delivery;

/**
 * E - the library's own Kafka publisher against a real broker: an outage on both lanes, an outage
 * mid-stream on two pods, a send timeout against the lease, and sends that outrun their bound.
 */
class KafkaIT {

  private static String[] kafka(String lane, String... more) {
    List<String> props =
        new ArrayList<>(
            List.of(
                "spring.kafka.bootstrap-servers=" + Infra.kafka().getBootstrapServers(),
                "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
                "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
                "opentmf.outbox.kafka.lane=" + lane,
                "opentmf.outbox.send-timeout=3s",
                "opentmf.outbox.kafka.lease=6s",
                "opentmf.outbox.ordered.lease=6s",
                "opentmf.outbox.backoff-base=500ms",
                "opentmf.outbox.backoff-cap=2s",
                "opentmf.outbox.max-attempts=100"));
    props.addAll(List.of(more));
    return props.toArray(String[]::new);
  }

  /** Per aggregate, the order the rows reached the topic in - id order expected. */
  private static void perAggregateOrder(
      Findings f, Map<Long, String> aggregateOf, boolean promised) {
    Map<String, List<Long>> seen = new LinkedHashMap<>();
    Receiver.deliveries().stream()
        .filter(d -> aggregateOf.containsKey(d.rowId())) // the topic's rows, not the probes
        .sorted(Comparator.comparing(Delivery::start))
        .forEach(
            d ->
                seen.computeIfAbsent(aggregateOf.get(d.rowId()), k -> new ArrayList<>())
                    .add(d.rowId()));
    int outOfOrder = 0;
    for (Map.Entry<String, List<Long>> e : seen.entrySet()) {
      List<Long> firsts = e.getValue().stream().distinct().toList();
      for (int i = 1; i < firsts.size(); i++) {
        if (firsts.get(i) < firsts.get(i - 1)) {
          outOfOrder++;
          if (outOfOrder <= 10 && promised) {
            f.add(
                "S",
                "aggregate %s reached the topic out of order: %s".formatted(e.getKey(), firsts));
          }
          break;
        }
      }
    }
    long duplicates =
        Receiver.deliveries().stream()
            .filter(d -> aggregateOf.containsKey(d.rowId()))
            .collect(Collectors.groupingBy(Delivery::rowId, Collectors.counting()))
            .values()
            .stream()
            .filter(n -> n > 1)
            .count();
    f.figure(
        "aggregates out of order=%d, rows on the topic more than once=%d"
            .formatted(outOfOrder, duplicates));
  }

  private static void outage(String lane, String name) {
    String topic = "topic-e1-" + UUID.randomUUID();
    try (Scenario s = Scenario.start(name, 2, kafka(lane));
        KafkaTap tap = new KafkaTap(topic, Lane.valueOf(lane))) {
      Map<Long, String> aggregateOf = new LinkedHashMap<>();
      for (int i = 0; i < 60; i++) {
        String aggregate = "agg-" + (i % 15);
        aggregateOf.put(
            s.pod(i % 2)
                .append(OutboxAppend.of("it", aggregate, "it.e1.v1", topic, Map.of("i", i))),
            aggregate);
      }
      await().atMost(Duration.ofSeconds(30)).until(() -> Receiver.deliveries().size() >= 60);
      Infra.pauseKafka(true); // the broker goes away
      try {
        for (int i = 60; i < 150; i++) {
          String aggregate = "agg-" + (i % 15);
          aggregateOf.put(
              s.pod(i % 2)
                  .append(OutboxAppend.of("it", aggregate, "it.e1.v1", topic, Map.of("i", i))),
              aggregate);
        }
        await().pollDelay(Duration.ofSeconds(20)).atMost(Duration.ofSeconds(25)).until(() -> true);
      } finally {
        Infra.pauseKafka(false); // and comes back
      }
      await()
          .atMost(Duration.ofMinutes(2))
          .until(
              () -> Receiver.deliveries().stream().map(Delivery::rowId).distinct().count() >= 150);
      Findings f =
          s.finish(
              Duration.ofMinutes(2),
              Expect.strict().atLeastOnce().skipStarvation().noOrderedIdOrder());
      // ORDERED across two pods interleaves - documented (README, kafka.lane); CONCURRENT promises
      // it
      perAggregateOrder(f, aggregateOf, lane.equals("CONCURRENT"));
      f.assertNone();
    }
  }

  /** E1 (ORDERED): the broker paused 20 s mid-stream; every row reaches the topic. */
  @Test
  void e1a_brokerDown_orderedLane() {
    outage("ORDERED", "E1a-broker-down-ordered");
  }

  /** E1 (CONCURRENT, per aggregate across pods): every row, each aggregate in append order. */
  @Test
  void e1b_brokerDown_concurrentLane_perAggregateOrder() {
    outage("CONCURRENT", "E1b-broker-down-concurrent");
  }

  /**
   * E3: a Kafka lease not longer than the send timeout - the at-least-once window the lease exists
   * to close - is refused at startup by validation (the publisher can never outlive its lease); the
   * library has no per-attempt header.
   */
  @Test
  void e3_aKafkaLeaseNotLongerThanTheSendTimeout_isRefusedAtStartup() {
    try (Scenario s = Scenario.start("E3-send-timeout-vs-lease", 0)) {
      assertThatThrownBy(
              () -> s.startPod("pod-a", kafka("CONCURRENT", "opentmf.outbox.kafka.lease=3s")))
          .hasStackTraceContaining("must exceed");
      Findings f = s.finish(Duration.ofSeconds(5), Expect.strict().noProbe());
      f.figure("startup refused: kafka.lease must exceed send-timeout");
      f.assertNone();
    }
  }

  /** The pod's {@code claims.paused} gauge for the Kafka transport; NaN while it has none. */
  private static double kafkaClaimsPaused(Pod pod) {
    Gauge gauge =
        pod.registry().find("opentmf.outbox.claims.paused").tag("transport", "kafka").gauge();
    return gauge == null ? Double.NaN : gauge.value();
  }

  /**
   * E4 (OUTBOX-INTERRUPTED-SEND-COUNTS-FOR-PAUSE-1): a pod whose transactional producer hangs in
   * {@code initTransactions} - the broker healthy - outruns every send's bound, and the heartbeat
   * interrupts each. Those failures count: the pod's Kafka claims pause within a bound or two.
   * Released, a probe succeeds, the pause ends and every row reaches the topic. Up to 1.11.0 the
   * interrupt (Kafka's {@code InterruptException}) never counted: the pod never paused.
   */
  @Test
  void e4_sendsThatOutrunTheirBound_pauseTheClaims_andAProbeResumesThem() {
    String topic = "topic-e4-" + UUID.randomUUID();
    KafkaHeldInit.hold();
    try (Scenario s =
            Scenario.start(
                "E4-interrupted-sends-pause",
                1,
                kafka(
                    "CONCURRENT",
                    "it.kafka.held-init=true",
                    "spring.kafka.producer.transaction-id-prefix=inv-e4-",
                    "spring.kafka.producer.properties[delivery.timeout.ms]=3000",
                    "spring.kafka.producer.properties[request.timeout.ms]=2000",
                    "spring.kafka.producer.properties[linger.ms]=0",
                    "spring.kafka.producer.properties[max.block.ms]=2000",
                    "opentmf.outbox.claim-pause.max=4s"));
        KafkaTap tap = new KafkaTap(topic, Lane.CONCURRENT)) {
      Pod pod = s.pod(0);
      for (int i = 0; i < 12; i++) {
        pod.append(OutboxAppend.of("it", "agg-" + i, "it.e4.v1", topic, Map.of("i", i)));
      }
      Findings f;
      try {
        long started = System.nanoTime();
        boolean paused =
            waitFor(() -> kafkaClaimsPaused(pod) == 1d, Duration.ofSeconds(60));
        long pausedAfter = (System.nanoTime() - started) / 1_000_000;
        KafkaHeldInit.release();
        boolean resumed = waitFor(() -> kafkaClaimsPaused(pod) == 0d, Duration.ofSeconds(30));
        await()
            .atMost(Duration.ofMinutes(1))
            .until(
                () -> Receiver.deliveries().stream().map(Delivery::rowId).distinct().count() >= 12);
        f = s.finish(Duration.ofMinutes(1), Expect.strict().skipStarvation());
        if (!paused) {
          f.add("E4", "sends that outran their bound for 60 s never paused the kafka claims");
        } else if (!resumed) {
          f.add("E4", "the kafka claims stayed paused 30 s after the producer was released");
        }
        f.figure("paused after %d ms, resumed=%s".formatted(paused ? pausedAfter : -1, resumed));
      } finally {
        KafkaHeldInit.release();
      }
      f.assertNone();
    }
  }

  private static boolean waitFor(BooleanSupplier condition, Duration atMost) {
    long deadline = System.nanoTime() + atMost.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      await().pollDelay(Duration.ofMillis(200)).atMost(Duration.ofSeconds(1)).until(() -> true);
    }
    return condition.getAsBoolean();
  }
}
