package org.opentmf.outboxinvariants;

import static org.awaitility.Awaitility.await;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxPruneResult;
import org.opentmf.outboxinvariants.Receiver.Delivery;
import org.opentmf.outboxinvariants.Receiver.Script;

/** D - the database: an outage mid-relay, the pool at its edge, a 200,000-row backlog. */
class DatabaseIT {

  /**
   * D1: the database link is cut for 20 s while two pods relay both lanes: the relays survive,
   * resume, every row is relayed, duplicates only where a send ended inside the outage (its booking
   * could not reach the database), and the gauges recover.
   */
  @Test
  void d1_aTwentySecondOutageMidRelay() {
    try (Scenario s =
        Scenario.start(
            "D1-db-outage-20s",
            2,
            "opentmf.outbox.lease=5s",
            "opentmf.outbox.ordered.lease=5s",
            "opentmf.outbox.send-timeout=2s",
            "spring.datasource.hikari.connection-timeout=2000")) {
      Receiver.script(
          "inv:d1c/", Script.concurrent().latency(Duration.ofMillis(5), Duration.ofMillis(30)));
      Receiver.script("inv:d1o/", Script.ordered().latency(Duration.ofMillis(10)));
      Scenario.insertConcurrent("inv:d1c/", 200, 10);
      for (int i = 0; i < 200; i++) {
        s.pod(i % 2).append("inv:d1o/q", "d1o-" + i);
      }
      await().atMost(Duration.ofSeconds(20)).until(() -> Receiver.deliveries().size() > 200);
      s.outage(Duration.ofSeconds(20));
      Findings f = s.finish(Duration.ofMinutes(3), Expect.strict().noOrderedIdOrder());
      f.assertNone();
    }
  }

  /** D2a: the pool at max-in-flight + 4 (the README's sizing, 1.9.0) under full load - no stall. */
  @Test
  void d2a_poolAtMaxInFlightPlusFour_underFullLoad() {
    pool("D2a-pool-max-in-flight-plus-4", 12);
  }

  /** D2b: the pool at max-in-flight + 2 (below the sizing) under full load - never a deadlock. */
  @Test
  void d2b_poolAtMaxInFlightPlusTwo_neverADeadlock() {
    pool("D2b-pool-max-in-flight-plus-2", 10);
  }

  private static void pool(String name, int poolSize) {
    try (Scenario s =
        Scenario.start(
            name,
            1,
            "spring.datasource.hikari.maximum-pool-size=" + poolSize,
            "spring.datasource.hikari.connection-timeout=5000",
            "opentmf.outbox.concurrent.max-in-flight=8",
            "opentmf.outbox.metrics-refresh=200ms")) {
      Receiver.script(
          "inv:d2c/", Script.concurrent().latency(Duration.ofMillis(10), Duration.ofMillis(40)));
      Receiver.script("inv:d2o/", Script.ordered().latency(Duration.ofMillis(10)));
      Scenario.insertConcurrent("inv:d2c/", 500, 6);
      for (int i = 0; i < 300; i++) {
        s.pod(0).append("inv:d2o/q", "d2o-" + i);
      }
      long started = System.nanoTime();
      Findings f = s.finish(Duration.ofMinutes(3), Expect.strict());
      f.figure(
          "drained in %d s with a pool of %d"
              .formatted((System.nanoTime() - started) / 1_000_000_000, poolSize));
      f.assertNone();
    }
  }

  /**
   * D3: 200,000 pending rows - the CONCURRENT claim reads a bounded number of buffers (EXPLAIN
   * ANALYZE BUFFERS of the library's own statement), and the prune running WHILE two pods relay
   * never deletes a row that is not relayed or cancelled.
   */
  @Test
  void d3_twoHundredThousandRows_boundedClaim_andAPruneWhileRelaying() throws Exception {
    try (Scenario s =
        Scenario.start(
            "D3-200k-claim-and-prune",
            0,
            "opentmf.outbox.retention=1s",
            "opentmf.outbox.maintenance.batch-size=5000")) {
      Receiver.script("inv:d3/", Script.concurrent());
      s.startPod("pod-a", "opentmf.outbox.concurrent.max-in-flight=1"); // migrate, barely relay
      Scenario.insertConcurrent("inv:d3/", 20000, 10);
      Scenario.sql("analyze outbox");
      long buffers = claimBuffers();
      Set<Long> before = ids();
      s.startPod("pod-b");
      AtomicBoolean pruning = new AtomicBoolean(true);
      AtomicLong pruned = new AtomicLong();
      CompletableFuture<Void> pruner =
          CompletableFuture.runAsync(
              () -> {
                while (pruning.get()) {
                  OutboxPruneResult result = s.pod(0).maintenance().pruneExpired();
                  pruned.addAndGet(result.pruned());
                }
              });
      try {
        await()
            .pollInterval(Duration.ofSeconds(1))
            .atMost(Duration.ofMinutes(8))
            .until(
                () ->
                    Scenario.count(
                            "relayed_on is null and cancelled_on is null and parked_on is null")
                        == 0);
      } finally {
        pruning.set(false);
        pruner.get();
      }
      Findings f = s.finish(Duration.ofMinutes(1), Expect.strict().pruned());
      f.figure(
          "claim over 200k pending rows read %d buffers; the prune deleted %d rows while relaying"
              .formatted(buffers, pruned.get()));
      if (buffers > 2500) {
        f.add(
            "S",
            "the CONCURRENT claim read %d buffers over 200k pending rows (bound 2,500)"
                .formatted(buffers));
      }
      Set<Long> after = ids();
      Set<Long> delivered =
          Receiver.deliveries().stream()
              .filter(Delivery::delivered)
              .map(Delivery::rowId)
              .collect(Collectors.toSet());
      Set<Long> gone = new HashSet<>(before);
      gone.removeAll(after);
      long prunedUndelivered = gone.stream().filter(id -> !delivered.contains(id)).count();
      if (prunedUndelivered > 0) {
        f.add(
            "S",
            "the prune deleted %d rows that were never delivered".formatted(prunedUndelivered));
      }
      f.assertNone();
    }
  }

  private static Set<Long> ids() throws SQLException {
    Set<Long> ids = new HashSet<>();
    try (Connection c = Infra.connect();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("select id from outbox")) {
      while (rs.next()) {
        ids.add(rs.getLong(1));
      }
    }
    return ids;
  }

  /** EXPLAIN (ANALYZE, BUFFERS) of the library's CONCURRENT claim, as a full claim of 100 rows. */
  private static long claimBuffers() throws Exception {
    Class<?> sqlClass = Class.forName("org.opentmf.outbox.internal.OutboxClaimSql");
    Field field = sqlClass.getDeclaredField("CONCURRENT");
    field.setAccessible(true);
    String statement = (String) field.get(null);
    Map<String, String> literals = Map.of("now", "now()", "after", "''", "limit", "100");
    Matcher named = Pattern.compile("(?<!:):(\\w+)").matcher(statement);
    StringBuilder sql = new StringBuilder();
    while (named.find()) {
      named.appendReplacement(sql, Matcher.quoteReplacement(literals.get(named.group(1))));
    }
    named.appendTail(sql);
    try (Connection c = Infra.connect();
        Statement st = c.createStatement()) {
      c.setAutoCommit(false);
      try (ResultSet rs = st.executeQuery("explain (analyze, buffers, format json) " + sql)) {
        rs.next();
        String json = rs.getString(1);
        Matcher hit = Pattern.compile("\"Shared Hit Blocks\": (\\d+)").matcher(json);
        Matcher read = Pattern.compile("\"Shared Read Blocks\": (\\d+)").matcher(json);
        long total = 0;
        if (hit.find()) {
          total += Long.parseLong(hit.group(1));
        }
        if (read.find()) {
          total += Long.parseLong(read.group(1));
        }
        return total;
      } finally {
        c.rollback();
      }
    }
  }
}
