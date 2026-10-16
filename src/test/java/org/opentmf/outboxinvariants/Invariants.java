package org.opentmf.outboxinvariants;

import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.awaitility.core.ConditionTimeoutException;
import org.opentmf.outbox.OutboxPublisher.ExhaustionOutcome;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.opentmf.outboxinvariants.Receiver.Delivery;
import org.opentmf.outboxinvariants.Receiver.Outcome;
import org.opentmf.outboxinvariants.Receiver.Script;
import org.springframework.boot.convert.DurationStyle;

/**
 * The six invariants, checked at the end of EVERY scenario against the outbox table and the
 * receiver's records; each violation becomes a finding, none stops the others.
 *
 * <ul>
 *   <li>I1 exactly once per lease: every row not cancelled, not parked is relayed, and every extra
 *       delivery is explained by a delivery that outlived its lease or a killed pod;
 *   <li>I2 order: one ORDERED send at a time per pod, ORDERED rows in id order per pod; never two
 *       rows of one key in flight at once, a key's rows in id order (a row that FAILED first may be
 *       passed - the documented exception, counted);
 *   <li>I3 no starvation: no row waits longer than one sweep plus its lease while a slot of its
 *       lane was free;
 *   <li>I4 the gauges and counters equal the table after quiescence;
 *   <li>I5 no thread death: every live pod's relay thread, ORDERED thread and a CONCURRENT slot run
 *       a task, and probe rows relay;
 *   <li>I6 state consistency: never relayed and parked, never sent after a cancel or before
 *       release_at, an exhausted PARK row is parked, no lease left after quiescence.
 * </ul>
 */
final class Invariants {

  private static final Duration SLACK = Duration.ofMillis(50);

  private Invariants() {}

  record Row(
      long id,
      String destination,
      String lane,
      String key,
      int attempts,
      Instant created,
      Instant releaseAt,
      Instant relayed,
      Instant cancelled,
      Instant parked,
      Instant claimedUntil,
      String lastError) {

    boolean concurrent() {
      return "CONCURRENT".equals(lane);
    }

    boolean open() {
      return relayed == null && cancelled == null && parked == null;
    }
  }

  static void check(Scenario scenario, Expect expect, Findings findings) {
    List<Row> rows = load();
    Map<Long, Row> byId = rows.stream().collect(Collectors.toMap(Row::id, r -> r));
    Map<Long, List<Delivery>> byRow =
        Receiver.deliveries().stream()
            .sorted(Comparator.comparing(Delivery::start))
            .collect(Collectors.groupingBy(Delivery::rowId));
    findings.figure("rows=" + rows.size() + " deliveries=" + Receiver.deliveries().size());
    if (Receiver.deliveries().size() <= 12) { // a small scenario: every delivery, and every row
      Receiver.deliveries().stream()
          .sorted(Comparator.comparing(Delivery::start))
          .forEach(
              d ->
                  findings.figure(
                      "delivery row %d %s#%d %s %s..%s"
                          .formatted(
                              d.rowId(), d.pod(), d.attempt(), d.outcome(), d.start(), d.end())));
      rows.forEach(
          r ->
              findings.figure(
                  "row %d attempts=%d relayed=%s claimed_until=%s last_error=%s"
                      .formatted(
                          r.id(), r.attempts(), r.relayed(), r.claimedUntil(), r.lastError())));
    }
    exactlyOncePerLease(scenario, expect, findings, rows, byRow);
    order(scenario, expect, findings, byId, byRow);
    if (!expect.skipStarvation) {
      noStarvation(scenario, expect, findings, rows, byId, byRow);
    }
    consistency(findings, rows, byRow);
    gauges(scenario, expect, findings, rows, byRow);
    if (expect.probe) {
      noThreadDeath(scenario, findings);
    }
  }

  // ---------------------------------------------------------------- I1

  private static void exactlyOncePerLease(
      Scenario scenario,
      Expect expect,
      Findings findings,
      List<Row> rows,
      Map<Long, List<Delivery>> byRow) {
    int duplicates = 0;
    int explained = 0;
    for (Row row : rows) {
      List<Delivery> deliveries = byRow.getOrDefault(row.id(), List.of());
      long delivered = deliveries.stream().filter(Delivery::delivered).count();
      if (row.open() && !expect.mayStayOpen.contains(row.id())) {
        findings.add(
            "I1",
            "row %d (%s) never relayed: attempts %d, %d deliveries, last_error %s"
                .formatted(
                    row.id(),
                    row.destination(),
                    row.attempts(),
                    deliveries.size(),
                    row.lastError()));
      }
      if (row.relayed() != null && delivered == 0 && deliveries.isEmpty() && tracked(row)) {
        findings.add("I1", "row %d booked relayed, the receiver never saw it".formatted(row.id()));
      }
      if (delivered > 1) {
        Duration lease = lease(scenario, row);
        long unbookable =
            deliveries.stream()
                .filter(
                    d ->
                        d.took().compareTo(lease) > 0
                            || scenario.killed.contains(d.pod())
                            || inOutage(scenario, d.end()))
                .count();
        long extra = delivered - 1;
        duplicates += (int) extra;
        explained += (int) Math.min(extra, unbookable);
        if (extra > unbookable && !expect.atLeastOnce && !expect.mayDuplicate.contains(row.id())) {
          findings.add(
              "I1",
              "row %d (%s) delivered %d times, %d explained by a lapsed lease (%s) or a killed pod: %s"
                  .formatted(
                      row.id(),
                      row.destination(),
                      delivered,
                      unbookable,
                      lease,
                      describe(deliveries)));
        }
      }
    }
    findings.figure("duplicates=" + duplicates + " (explained " + explained + ")");
  }

  /**
   * A send that ended while the database was cut - or just before (its booking was on the wire when
   * the link went) or just after (the pool reconnecting) - could not be booked.
   */
  private static boolean inOutage(Scenario scenario, Instant end) {
    return scenario.outages.stream()
        .anyMatch(o -> !end.isBefore(o[0].minusSeconds(1)) && !end.isAfter(o[1].plusSeconds(1)));
  }

  /** A row the receiver records (the scripted, HTTP and Kafka receivers all report here). */
  private static boolean tracked(Row row) {
    return row.destination().startsWith(ScriptedPublisher.PREFIX)
        || row.destination().startsWith("http")
        || row.destination().startsWith("topic-");
  }

  // ---------------------------------------------------------------- I2

  private static void order(
      Scenario scenario,
      Expect expect,
      Findings findings,
      Map<Long, Row> byId,
      Map<Long, List<Delivery>> byRow) {
    List<Delivery> all =
        Receiver.deliveries().stream().sorted(Comparator.comparing(Delivery::start)).toList();
    // one ORDERED send at a time per pod
    Map<String, List<Delivery>> orderedByPod =
        all.stream()
            .filter(d -> d.lane() == Lane.ORDERED)
            .collect(Collectors.groupingBy(Delivery::pod));
    orderedByPod.forEach(
        (pod, sends) -> {
          for (int i = 1; i < sends.size(); i++) {
            Delivery before = sends.get(i - 1);
            Delivery after = sends.get(i);
            if (after.start().isBefore(before.end().minus(SLACK))) {
              findings.add(
                  "I2",
                  "pod %s sent ORDERED rows %d and %d at once"
                      .formatted(pod, before.rowId(), after.rowId()));
            }
          }
          if (expect.orderedIdOrder) {
            long last = -1;
            for (Delivery d : sends) {
              Row row = byId.get(d.rowId());
              if (row == null
                  || d.attempt() != 1
                  || row.releaseAt() != null
                  || failedEver(byRow, row)) {
                continue;
              }
              if (d.rowId() < last) {
                findings.add(
                    "I2",
                    "pod %s sent ORDERED row %d after row %d".formatted(pod, d.rowId(), last));
              }
              last = Math.max(last, d.rowId());
            }
          }
        });
    // keyed CONCURRENT: never two in flight, id order on the happy path
    Map<String, List<Delivery>> byKey =
        all.stream()
            .filter(d -> d.lane() == Lane.CONCURRENT && d.key() != null)
            .collect(Collectors.groupingBy(Delivery::key));
    int passes = 0;
    for (Map.Entry<String, List<Delivery>> entry : byKey.entrySet()) {
      List<Delivery> sends = entry.getValue();
      Delivery longest = null;
      for (Delivery d : sends) {
        if (longest != null && d.start().isBefore(longest.end().minus(SLACK))) {
          Row earlier = byId.get(longest.rowId());
          boolean lapsed =
              earlier != null && longest.took().compareTo(lease(scenario, earlier)) > 0;
          if (!lapsed && !scenario.killed.contains(longest.pod())) {
            findings.add(
                "I2",
                "key %s had rows %d and %d in flight at once (%s, %s)"
                    .formatted(entry.getKey(), longest.rowId(), d.rowId(), longest.pod(), d.pod()));
          }
        }
        if (longest == null || d.end().isAfter(longest.end())) {
          longest = d;
        }
      }
      List<Delivery> firstDelivered = new ArrayList<>();
      Map<Long, Boolean> seen = new HashMap<>();
      for (Delivery d : sends) {
        if (d.delivered() && seen.putIfAbsent(d.rowId(), true) == null) {
          firstDelivered.add(d);
        }
      }
      for (int i = 1; i < firstDelivered.size(); i++) {
        Delivery later = firstDelivered.get(i);
        Delivery earlier = firstDelivered.get(i - 1);
        if (later.rowId() < earlier.rowId()) {
          Row passed = byId.get(later.rowId());
          boolean failedBefore =
              byRow.getOrDefault(later.rowId(), List.of()).stream()
                  .anyMatch(x -> !x.delivered() && x.end().isBefore(earlier.start().plus(SLACK)));
          // held when the row that passed it was CLAIMED - which is up to its queue wait before its
          // send: the claim instant is its first send's stamp less its lease (the send itself
          // where no scripted publisher saw the stamp)
          Instant passerClaimed = earlier.start();
          OffsetDateTime stamp = Receiver.firstStamp(earlier.rowId());
          Row passer = byId.get(earlier.rowId());
          if (stamp != null && passer != null) {
            Instant claimed = stamp.toInstant().minus(lease(scenario, passer));
            passerClaimed = claimed.isBefore(passerClaimed) ? claimed : passerClaimed;
          }
          boolean held =
              passed != null
                  && passed.releaseAt() != null
                  && passed.releaseAt().isAfter(passerClaimed.minus(SLACK));
          boolean failedInTable = passed != null && passed.attempts() > 0;
          if (failedBefore || held || failedInTable) {
            passes++;
          } else {
            findings.add(
                "I2",
                "key %s: row %d delivered after row %d - out of id order (row %d: release_at %s, attempts %d, first delivered %s; row %d claimed %s, sent %s)"
                    .formatted(
                        entry.getKey(),
                        later.rowId(),
                        earlier.rowId(),
                        later.rowId(),
                        passed == null ? "?" : passed.releaseAt(),
                        passed == null ? -1 : passed.attempts(),
                        later.start(),
                        earlier.rowId(),
                        passerClaimed,
                        earlier.start()));
          }
        }
      }
    }
    findings.figure("documented key pass-overs (a failed or held row passed)=" + passes);
  }

  private static boolean failedEver(Map<Long, List<Delivery>> byRow, Row row) {
    return byRow.getOrDefault(row.id(), List.of()).stream().anyMatch(d -> !d.delivered());
  }

  // ---------------------------------------------------------------- I3

  private static void noStarvation(
      Scenario scenario,
      Expect expect,
      Findings findings,
      List<Row> rows,
      Map<Long, Row> byId,
      Map<Long, List<Delivery>> byRow) {
    Duration sweep = duration(scenario, "opentmf.outbox.sweep-interval", "5s");
    int slots =
        Integer.parseInt(
            scenario.properties.getOrDefault("opentmf.outbox.concurrent.max-in-flight", "8"));
    Instant now = Instant.now();
    FreeTime concurrentFree = FreeTime.of(scenario, now, slots, true);
    FreeTime orderedFree = FreeTime.of(scenario, now, 1, false);
    Duration maxWaitConcurrent = Duration.ZERO;
    Duration maxWaitOrdered = Duration.ZERO;
    Duration maxFree = Duration.ZERO;
    Map<String, List<Row>> rowsByKey =
        rows.stream().filter(r -> r.key() != null).collect(Collectors.groupingBy(Row::key));
    for (Row row : rows) {
      if (row.cancelled() != null || !tracked(row)) {
        continue;
      }
      List<Delivery> deliveries = byRow.getOrDefault(row.id(), List.of());
      Instant firstStart =
          deliveries.isEmpty() ? (row.open() ? now : null) : deliveries.get(0).start();
      if (firstStart == null) {
        continue;
      }
      Instant eligible =
          row.releaseAt() != null && row.releaseAt().isAfter(row.created())
              ? row.releaseAt()
              : row.created();
      if (row.key() != null) { // behind its key's earlier rows
        for (Row other : rowsByKey.getOrDefault(row.key(), List.of())) {
          if (other.id() < row.id()) {
            List<Delivery> theirs = byRow.getOrDefault(other.id(), List.of());
            Instant done = theirs.isEmpty() ? null : theirs.get(theirs.size() - 1).end();
            if (done == null && other.open()) {
              eligible = firstStart; // its key's head never went: not starvation
            } else if (done != null && done.isAfter(eligible)) {
              eligible = done;
            }
          }
        }
      }
      if (!firstStart.isAfter(eligible)) {
        continue;
      }
      Duration wait = Duration.between(eligible, firstStart);
      Duration free =
          (row.concurrent() ? concurrentFree : orderedFree).between(eligible, firstStart);
      if (row.concurrent()) {
        maxWaitConcurrent = max(maxWaitConcurrent, wait);
      } else {
        maxWaitOrdered = max(maxWaitOrdered, wait);
      }
      maxFree = max(maxFree, free);
      Duration bound =
          expect.starvationBound != null
              ? expect.starvationBound
              : sweep.plus(lease(scenario, row)).plusSeconds(1);
      if (free.compareTo(bound) > 0) {
        StringBuilder perPod = new StringBuilder();
        for (Pod pod : scenario.pods) {
          FreeTime own =
              FreeTime.of(
                  scenario, now, row.concurrent() ? slots : 1, row.concurrent(), pod.name());
          perPod
              .append(' ')
              .append(pod.name())
              .append('=')
              .append(own.between(eligible, firstStart).toMillis())
              .append("ms");
        }
        findings.add(
            "I3",
            "row %d (%s) waited %s, %s of it with a free %s slot (bound %s); sent by %s; free per pod:%s"
                .formatted(
                    row.id(),
                    row.destination(),
                    wait,
                    free,
                    row.concurrent() ? "CONCURRENT" : "ORDERED",
                    bound,
                    deliveries.isEmpty() ? "-" : deliveries.get(0).pod(),
                    perPod));
      }
    }
    findings.figure(
        "max wait ORDERED=%dms CONCURRENT=%dms, max wait with a free slot=%dms"
            .formatted(
                maxWaitOrdered.toMillis(), maxWaitConcurrent.toMillis(), maxFree.toMillis()));
  }

  private static Duration max(Duration a, Duration b) {
    return a.compareTo(b) >= 0 ? a : b;
  }

  /** When some live pod had a free slot of one lane: a cumulative step function. */
  static final class FreeTime {
    private final List<Instant[]> intervals = new ArrayList<>();
    private final List<Long> cumulative = new ArrayList<>();

    /** Outages and the reconnect after them: free slots then are no starvation. */
    private final List<Instant[]> outages = new ArrayList<>();

    static FreeTime of(Scenario scenario, Instant now, int slots, boolean concurrent) {
      return of(scenario, now, slots, concurrent, null);
    }

    static FreeTime of(
        Scenario scenario, Instant now, int slots, boolean concurrent, String onlyPod) {
      FreeTime free = new FreeTime();
      for (Instant[] outage : scenario.outages) {
        free.outages.add(new Instant[] {outage[0], outage[1].plusSeconds(3)});
      }
      List<Instant[]> perPodFree = new ArrayList<>();
      for (Pod pod : scenario.pods) {
        if (onlyPod != null && !pod.name().equals(onlyPod)) {
          continue;
        }
        Instant from = pod.startedAt;
        Instant to = pod.stoppedAt != null ? pod.stoppedAt : now;
        List<Delivery> mine =
            Receiver.deliveries().stream()
                .filter(d -> d.pod().equals(pod.name()))
                .filter(d -> concurrent == (d.lane() == Lane.CONCURRENT))
                .toList();
        List<Object[]> events = new ArrayList<>();
        for (Delivery d : mine) {
          events.add(new Object[] {d.start(), 1});
          events.add(new Object[] {d.end(), -1});
        }
        events.sort(Comparator.comparing(e -> (Instant) e[0]));
        int busy = 0;
        Instant cursor = from;
        for (Object[] e : events) {
          Instant at = (Instant) e[0];
          if (at.isBefore(from)) {
            busy += (int) e[1];
            continue;
          }
          if (at.isAfter(to)) {
            break;
          }
          int podSlots = concurrent ? pod.slots : slots;
          if (busy < podSlots && at.isAfter(cursor)) {
            perPodFree.add(new Instant[] {cursor, at});
          }
          busy += (int) e[1];
          cursor = at;
        }
        if (busy < (concurrent ? pod.slots : slots) && to.isAfter(cursor)) {
          perPodFree.add(new Instant[] {cursor, to});
        }
      }
      perPodFree.sort(Comparator.comparing(i -> i[0]));
      for (Instant[] interval : perPodFree) { // union
        if (!free.intervals.isEmpty()) {
          Instant[] last = free.intervals.get(free.intervals.size() - 1);
          if (!interval[0].isAfter(last[1])) {
            if (interval[1].isAfter(last[1])) {
              last[1] = interval[1];
            }
            continue;
          }
        }
        free.intervals.add(new Instant[] {interval[0], interval[1]});
      }
      long sum = 0;
      for (Instant[] interval : free.intervals) {
        sum += Duration.between(interval[0], interval[1]).toNanos();
        free.cumulative.add(sum);
      }
      return free;
    }

    /** Free time within [from, to], less any database outage (no pod can claim then). */
    Duration between(Instant from, Instant to) {
      long free = upTo(to) - upTo(from);
      for (Instant[] outage : outages) {
        Instant a = outage[0].isAfter(from) ? outage[0] : from;
        Instant b = outage[1].isBefore(to) ? outage[1] : to;
        if (b.isAfter(a)) {
          free -= Math.min(free, Duration.ofNanos(upTo(b) - upTo(a)).toNanos());
        }
      }
      return Duration.ofNanos(Math.max(0, free));
    }

    private long upTo(Instant t) {
      int lo = 0;
      int hi = intervals.size() - 1;
      int idx = -1;
      while (lo <= hi) { // the last interval starting at or before t
        int mid = (lo + hi) >>> 1;
        if (!intervals.get(mid)[0].isAfter(t)) {
          idx = mid;
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      if (idx < 0) {
        return 0;
      }
      long before = idx == 0 ? 0 : cumulative.get(idx - 1);
      Instant[] interval = intervals.get(idx);
      Instant end = interval[1].isBefore(t) ? interval[1] : t;
      return before + Duration.between(interval[0], end).toNanos();
    }
  }

  // ---------------------------------------------------------------- I6

  private static void consistency(
      Findings findings, List<Row> rows, Map<Long, List<Delivery>> byRow) {
    Instant dbNow = dbNow();
    for (Row row : rows) {
      List<Delivery> deliveries = byRow.getOrDefault(row.id(), List.of());
      if (row.relayed() != null && row.parked() != null) {
        findings.add("I6", "row %d is both relayed and parked".formatted(row.id()));
      }
      if (row.cancelled() != null) {
        deliveries.stream()
            .filter(d -> d.start().isAfter(row.cancelled().plus(SLACK)))
            .findFirst()
            .ifPresent(
                d ->
                    findings.add(
                        "I6",
                        "row %d sent at %s, after its cancel at %s"
                            .formatted(row.id(), d.start(), row.cancelled())));
      }
      if (row.releaseAt() != null) {
        deliveries.stream()
            .filter(d -> d.start().isBefore(row.releaseAt().minus(SLACK)))
            .findFirst()
            .ifPresent(
                d ->
                    findings.add(
                        "I6",
                        "row %d sent at %s, before its release_at %s"
                            .formatted(row.id(), d.start(), row.releaseAt())));
      }
      // a park is justified by a booked failure: the receiver's, or the library's own - a refusal
      // before any request (an unknown profile), a booking hook or listener that refused, a
      // counted lapse - each books attempts and last_error
      boolean failureBooked = row.attempts() > 0 && row.lastError() != null;
      if (row.parked() != null
          && !failureBooked
          && deliveries.stream().noneMatch(d -> !d.delivered())
          && tracked(row)) {
        findings.add("I6", "row %d parked without a failed attempt".formatted(row.id()));
      }
      if (row.destination().startsWith(ScriptedPublisher.PREFIX)
          && row.relayed() == null
          && row.cancelled() == null) {
        Script script = Receiver.scriptFor(row.destination());
        long failures = deliveries.stream().filter(d -> !d.delivered()).count();
        boolean terminal = deliveries.stream().anyMatch(d -> d.outcome() == Outcome.TERMINAL);
        boolean spent = script.maxAttempts > 0 && failures >= script.maxAttempts;
        if ((terminal || spent)
            && script.exhausted == ExhaustionOutcome.PARK
            && row.parked() == null) {
          findings.add(
              "I6",
              "row %d exhausted (%d failures%s) but not parked"
                  .formatted(row.id(), failures, terminal ? ", terminal" : ""));
        }
      }
      if (row.claimedUntil() != null
          && row.relayed() == null
          && row.claimedUntil().isAfter(dbNow)) {
        findings.add(
            "I6",
            "row %d still leased until %s after quiescence"
                .formatted(row.id(), row.claimedUntil()));
      }
    }
  }

  // ---------------------------------------------------------------- I4

  private static void gauges(
      Scenario scenario,
      Expect expect,
      Findings findings,
      List<Row> rows,
      Map<Long, List<Delivery>> byRow) {
    Instant dbNow = dbNow();
    Map<String, long[]> sql = new HashMap<>(); // lane -> pending, parked, inFlight
    for (String lane : List.of("ordered", "concurrent")) {
      sql.put(lane, new long[3]);
    }
    for (Row row : rows) {
      long[] counts = sql.get(row.concurrent() ? "concurrent" : "ordered");
      if (row.relayed() == null && row.cancelled() == null) {
        counts[0]++;
        if (row.parked() != null) {
          counts[1]++;
        }
      }
      if (row.claimedUntil() != null
          && row.relayed() == null
          && row.cancelled() == null
          && row.claimedUntil().isAfter(dbNow)) {
        counts[2]++;
      }
    }
    for (Pod pod : scenario.pods) {
      if (!pod.alive()) {
        continue;
      }
      try {
        pod.refreshGauges();
      } catch (RuntimeException ex) {
        findings.add("I4", "pod %s: the gauge refresh failed: %s".formatted(pod, ex));
        continue;
      }
      MeterRegistry registry = pod.registry();
      String[] names = {
        "opentmf.outbox.pending", "opentmf.outbox.parked", "opentmf.outbox.in-flight"
      };
      for (String lane : List.of("ordered", "concurrent")) {
        for (int i = 0; i < names.length; i++) {
          double gauge = registry.get(names[i]).tag("lane", lane).gauge().value();
          if (Double.isNaN(gauge) || (long) gauge != sql.get(lane)[i]) {
            findings.add(
                "I4",
                "pod %s: %s{lane=%s} = %s, the table says %d"
                    .formatted(pod, names[i], lane, gauge, sql.get(lane)[i]));
          }
        }
        double lag = registry.get("opentmf.outbox.relay-lag").tag("lane", lane).gauge().value();
        if (sql.get(lane)[0] == 0 && lag > 1) {
          findings.add(
              "I4", "pod %s: relay-lag{lane=%s} = %ss with nothing open".formatted(pod, lane, lag));
        }
      }
      double age = registry.get("opentmf.outbox.metrics-age").gauge().value();
      if (Double.isNaN(age) || age > 10) {
        findings.add("I4", "pod %s: metrics-age = %s right after a refresh".formatted(pod, age));
      }
    }
    // the counters, summed over the pods of this JVM
    if (!scenario.children.isEmpty() || expect.pruned) {
      findings.figure(
          "counters not compared: "
              + (expect.pruned
                  ? "rows were pruned"
                  : "a child-JVM pod's registry is out of reach"));
      return;
    }
    double relayedCounter = 0;
    double droppedCounter = 0;
    long attemptsCount = 0;
    for (Pod pod : scenario.pods) {
      MeterRegistry registry = pod.registryHandle;
      relayedCounter +=
          registry.find("opentmf.outbox.relayed").counters().stream()
              .mapToDouble(Counter::count)
              .sum();
      droppedCounter +=
          registry.find("opentmf.outbox.dropped").counters().stream()
              .mapToDouble(Counter::count)
              .sum();
      attemptsCount +=
          registry.find("opentmf.outbox.attempts").summaries().stream()
              .mapToLong(DistributionSummary::count)
              .sum();
    }
    long relayedRows = 0;
    long droppedRows = 0;
    for (Row row : rows) {
      if (row.relayed() == null) {
        continue;
      }
      List<Delivery> deliveries = byRow.getOrDefault(row.id(), List.of());
      boolean delivered = deliveries.stream().anyMatch(Delivery::delivered);
      if (!deliveries.isEmpty() && !delivered) {
        droppedRows++;
      } else {
        relayedRows++;
      }
    }
    // a stopped pod's context closed its registry - graceful or not: Scenario.stop marks the pod
    // killed for I1's sake, and its drain still books into the closed registry
    boolean stopped = scenario.pods.stream().anyMatch(p -> p.stoppedAt != null);
    if (!scenario.outages.isEmpty()
        && (long) relayedCounter < relayedRows
        && relayedRows - (long) relayedCounter <= 8L * scenario.pods.size()) {
      findings.figure(
          "relayed counter %s < %d table rows: a booking committed while its acknowledgement was lost in the outage counts nothing"
              .formatted(relayedCounter, relayedRows));
    } else if (stopped && (long) relayedCounter < relayedRows && attemptsCount == relayedRows) {
      findings.figure(
          "relayed counter %s < %d table rows: a stopped pod's drain booked into a closed registry (Boot closes it at ContextClosedEvent); the attempts summary has all %d"
              .formatted(relayedCounter, relayedRows, attemptsCount));
    } else if ((long) relayedCounter != relayedRows) {
      findings.add(
          "I4",
          "relayed counter = %s over the pods, the table relayed %d rows"
              .formatted(relayedCounter, relayedRows));
    }
    if ((long) droppedCounter != droppedRows) {
      findings.add(
          "I4",
          "dropped counter = %s over the pods, the table dropped %d rows"
              .formatted(droppedCounter, droppedRows));
    }
    if (expect.exactCounters && !stopped && attemptsCount != (long) relayedCounter) {
      findings.add(
          "I4",
          "attempts summary counted %d relays, the relayed counter %s"
              .formatted(attemptsCount, relayedCounter));
    }
  }

  // ---------------------------------------------------------------- I5

  private static void noThreadDeath(Scenario scenario, Findings findings) {
    scenario.uncaught.forEach(error -> findings.add("I5", "uncaught: " + error));
    Receiver.script("inv:probe/", Script.concurrent().latency(Duration.ZERO));
    Receiver.script("inv:probe-ordered/", Script.ordered().latency(Duration.ZERO));
    List<Long> probes = new ArrayList<>();
    for (Pod pod : scenario.pods) {
      if (!pod.alive()) {
        continue;
      }
      for (String field : List.of("outboxRelay:executor", "outboxOrderedLane:executor")) {
        String[] parts = field.split(":");
        if (!runsATask(pod, parts[0], parts[1])) {
          findings.add(
              "I5", "pod %s: %s's thread runs no task within 10 s".formatted(pod, parts[0]));
        }
      }
      if (!aSlotRunsATask(pod)) {
        findings.add("I5", "pod %s: no CONCURRENT slot runs a task within 10 s".formatted(pod));
      }
      try {
        probes.add(pod.append("inv:probe/" + pod.name(), "probe-" + pod.name()));
        probes.add(pod.append("inv:probe-ordered/" + pod.name(), "probe-" + pod.name()));
      } catch (RuntimeException ex) {
        findings.add("I5", "pod %s cannot append a probe row: %s".formatted(pod, ex));
      }
    }
    if (probes.isEmpty()) {
      return;
    }
    String ids = probes.stream().map(String::valueOf).collect(Collectors.joining(","));
    try {
      await()
          .pollInterval(Duration.ofMillis(100))
          .atMost(Duration.ofSeconds(30))
          .until(
              () ->
                  Scenario.count("id in (" + ids + ") and relayed_on is not null")
                      == probes.size());
    } catch (ConditionTimeoutException ex) {
      findings.add(
          "I5",
          "probe rows %s: %d of %d relayed within 30 s"
              .formatted(
                  ids,
                  Scenario.count("id in (" + ids + ") and relayed_on is not null"),
                  probes.size()));
    }
  }

  /** Submits a task to a pod bean's executor field; whether it runs within 10 s. */
  private static boolean runsATask(Pod pod, String bean, String field) {
    try {
      Object target = pod.context.getBean(bean);
      Field executorField = target.getClass().getDeclaredField(field);
      executorField.setAccessible(true);
      Object executor = executorField.get(target);
      if (executor == null) {
        return true; // never started (the ORDERED lane before its first batch): nothing died
      }
      CountDownLatch ran = new CountDownLatch(1);
      ((Executor) executor).execute(ran::countDown);
      return ran.await(10, TimeUnit.SECONDS);
    } catch (ReflectiveOperationException | RuntimeException ex) {
      System.out.println("INVARIANTS: I5 probe of " + bean + " on " + pod + ": " + ex);
      return false;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private static boolean aSlotRunsATask(Pod pod) {
    try {
      Object lane = pod.context.getBean("outboxConcurrentLane");
      Method submit = lane.getClass().getDeclaredMethod("submit", Runnable.class);
      submit.setAccessible(true);
      CountDownLatch ran = new CountDownLatch(1);
      Runnable task = ran::countDown;
      if (!(boolean) submit.invoke(lane, task)) {
        return false;
      }
      return ran.await(10, TimeUnit.SECONDS);
    } catch (ReflectiveOperationException | RuntimeException ex) {
      System.out.println("INVARIANTS: I5 slot probe on " + pod + ": " + ex);
      return false;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  // ---------------------------------------------------------------- support

  static Duration lease(Scenario scenario, Row row) {
    if (row.destination().startsWith(ScriptedPublisher.PREFIX)) {
      Script script = Receiver.scriptFor(row.destination());
      if (script.lease != null) {
        return script.lease;
      }
    }
    if (!row.concurrent()) {
      return duration(scenario, "opentmf.outbox.ordered.lease", "15s");
    }
    if (row.destination().startsWith("topic-")) {
      return duration(scenario, "opentmf.outbox.kafka.lease", "15s");
    }
    return duration(scenario, "opentmf.outbox.lease", "2m");
  }

  static Duration duration(Scenario scenario, String property, String fallback) {
    return DurationStyle.detectAndParse(scenario.properties.getOrDefault(property, fallback));
  }

  private static String describe(List<Delivery> deliveries) {
    return deliveries.stream()
        .map(
            d ->
                "%s#%d %s %dms%s"
                    .formatted(
                        d.pod(),
                        d.attempt(),
                        d.outcome(),
                        d.took().toMillis(),
                        d.interrupted() ? " interrupted" : ""))
        .collect(Collectors.joining(", "));
  }

  private static Instant dbNow() {
    try (Connection c = Infra.connect();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("select now()")) {
      rs.next();
      return rs.getTimestamp(1).toInstant();
    } catch (SQLException ex) {
      throw new IllegalStateException(ex);
    }
  }

  static List<Row> load() {
    List<Row> rows = new ArrayList<>();
    try (Connection c = Infra.connect();
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "select id, destination, lane, ordering_key, attempts, created_on, release_at,"
                    + " relayed_on, cancelled_on, parked_on, claimed_until, last_error from outbox"
                    + " order by id")) {
      while (rs.next()) {
        rows.add(
            new Row(
                rs.getLong(1),
                rs.getString(2),
                rs.getString(3),
                rs.getString(4),
                rs.getInt(5),
                instant(rs.getTimestamp(6)),
                instant(rs.getTimestamp(7)),
                instant(rs.getTimestamp(8)),
                instant(rs.getTimestamp(9)),
                instant(rs.getTimestamp(10)),
                instant(rs.getTimestamp(11)),
                rs.getString(12)));
      }
    } catch (SQLException ex) {
      throw new IllegalStateException(ex);
    }
    return rows;
  }

  private static Instant instant(Timestamp timestamp) {
    return timestamp == null ? null : timestamp.toInstant();
  }
}
