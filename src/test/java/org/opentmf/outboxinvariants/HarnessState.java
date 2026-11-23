package org.opentmf.outboxinvariants;

import com.github.dockerjava.api.model.Container;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.opentmf.outboxinvariants.Receiver.Delivery;
import org.testcontainers.DockerClientFactory;

/**
 * Information only, never a finding: the state of the harness's JVM around a long scenario, to
 * tell a scenario slowed by what earlier scenarios left behind (threads, pools, containers) from
 * one slowed by the relay itself. D3 prints it at its start and end, with its per-pod drain.
 */
final class HarnessState {

  private HarnessState() {}

  /** Live threads (by name family), every pod's pool, and this session's running containers. */
  static void print(String label, Scenario scenario) {
    Map<String, Long> families =
        Thread.getAllStackTraces().keySet().stream()
            .collect(
                Collectors.groupingBy(
                    t -> t.getName().replaceAll("[-#]?\\d+", "#"),
                    TreeMap::new,
                    Collectors.counting()));
    String top =
        families.entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .limit(12)
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(", "));
    System.out.printf(
        "HARNESS-STATE %s: live threads %d (peak %d); families: %s%n",
        label,
        ManagementFactory.getThreadMXBean().getThreadCount(),
        ManagementFactory.getThreadMXBean().getPeakThreadCount(),
        top);
    for (Pod pod : scenario.pods()) {
      System.out.printf("HARNESS-STATE %s: pod %s pool %s%n", label, pod.name(), pool(pod));
    }
    System.out.printf("HARNESS-STATE %s: running containers %s%n", label, containers());
  }

  /** Per pod: rows delivered, first and last delivery, and the longest pause between two. */
  static void printDrain(String label, List<Delivery> deliveries) {
    Map<String, List<Delivery>> byPod =
        deliveries.stream()
            .filter(Delivery::delivered)
            .collect(Collectors.groupingBy(Delivery::pod, TreeMap::new, Collectors.toList()));
    byPod.forEach(
        (pod, rows) -> {
          List<Instant> ends = rows.stream().map(Delivery::end).sorted().toList();
          Duration longest = Duration.ZERO;
          Instant longestAt = ends.get(0);
          for (int i = 1; i < ends.size(); i++) {
            Duration gap = Duration.between(ends.get(i - 1), ends.get(i));
            if (gap.compareTo(longest) > 0) {
              longest = gap;
              longestAt = ends.get(i - 1);
            }
          }
          System.out.printf(
              "HARNESS-STATE %s: pod %s delivered %d rows, first %s, last %s, longest pause %d ms"
                  + " after %s%n",
              label,
              pod,
              rows.size(),
              ends.get(0),
              ends.get(ends.size() - 1),
              longest.toMillis(),
              longestAt);
        });
  }

  private static String pool(Pod pod) {
    if (!pod.alive()) {
      return "closed";
    }
    try {
      DataSource dataSource = pod.bean(DataSource.class);
      HikariDataSource hikari = dataSource.unwrap(HikariDataSource.class);
      HikariPoolMXBean mx = hikari.getHikariPoolMXBean();
      return mx == null
          ? "not started"
          : "active %d idle %d total %d awaiting %d"
              .formatted(
                  mx.getActiveConnections(),
                  mx.getIdleConnections(),
                  mx.getTotalConnections(),
                  mx.getThreadsAwaitingConnection());
    } catch (Exception e) {
      return "unreadable (" + e.getClass().getSimpleName() + ")";
    }
  }

  private static String containers() {
    try {
      List<String> names =
          DockerClientFactory.instance()
              .client()
              .listContainersCmd()
              .withLabelFilter(
                  Map.of(
                      DockerClientFactory.TESTCONTAINERS_SESSION_ID_LABEL,
                      DockerClientFactory.SESSION_ID))
              .exec()
              .stream()
              .map(Container::getImage)
              .sorted(Comparator.naturalOrder())
              .toList();
      return names.size() + " " + names;
    } catch (Exception e) {
      return "unreadable (" + e.getClass().getSimpleName() + ")";
    }
  }
}
