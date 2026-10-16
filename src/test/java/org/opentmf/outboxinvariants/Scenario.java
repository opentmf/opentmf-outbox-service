package org.opentmf.outboxinvariants;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * One scenario: a seed, N pods on the shared database, the receiver reset; at the end the relay is
 * driven to QUIESCENCE and the six invariants are checked against the table and the receiver.
 * Findings are collected - every one, never the first only - and the run is written to {@code
 * target/invariants-report.tsv} before the scenario asserts.
 *
 * <p>The seed is {@code -Dit.seed} when given (re-run a failure exactly), else drawn and printed.
 */
public final class Scenario implements AutoCloseable {

  /** Properties every pod gets; a scenario overrides any of them. */
  static final Map<String, String> DEFAULTS =
      Map.ofEntries(
          Map.entry("spring.main.banner-mode", "off"),
          Map.entry("spring.main.web-application-type", "none"),
          Map.entry("spring.liquibase.change-log", "classpath:db/test-changelog.xml"),
          Map.entry("spring.jpa.hibernate.ddl-auto", "validate"),
          Map.entry("spring.datasource.hikari.maximum-pool-size", "16"),
          Map.entry("opentmf.outbox.sweep-interval", "1s"),
          Map.entry("opentmf.outbox.metrics-refresh", "1h"),
          Map.entry("logging.level.org.apache.kafka", "WARN"),
          Map.entry("logging.level.org.springframework", "WARN"),
          Map.entry("logging.level.org.hibernate", "WARN"),
          Map.entry("logging.level.liquibase", "WARN"),
          Map.entry("logging.level.com.zaxxer", "WARN"));

  final String name;
  final long seed;
  final Map<String, String> properties = new LinkedHashMap<>(DEFAULTS);
  final List<Pod> pods = new ArrayList<>();
  final Set<String> killed = ConcurrentHashMap.newKeySet();
  final List<String> notes = new ArrayList<>();
  final List<ChildPod> children = new ArrayList<>();

  /** Database outages [from, to]: a send that ended inside one could not book (redelivery). */
  final List<Instant[]> outages = new ArrayList<>();

  /** Throwables that escaped a thread (the relay's own threads first of all): each is a finding. */
  final List<String> uncaught = new CopyOnWriteArrayList<>();

  private final Thread.UncaughtExceptionHandler previousHandler =
      Thread.getDefaultUncaughtExceptionHandler();

  private Scenario(String name, long seed) {
    this.name = name;
    this.seed = seed;
  }

  /** A scenario named {@code name} with {@code pods} pods and these property overrides. */
  public static Scenario start(String name, int podCount, String... overrides) {
    long seed = Long.getLong("it.seed", System.nanoTime());
    System.out.printf(
        "INVARIANTS: scenario %s seed %d (re-run with -Dit.seed=%d)%n", name, seed, seed);
    Infra.start();
    Infra.healDatabase();
    Infra.truncate();
    Receiver.reset(seed);
    Scenario scenario = new Scenario(name, seed);
    Awaitility.doNotCatchUncaughtExceptionsByDefault(); // they are findings, not a test error
    Thread.setDefaultUncaughtExceptionHandler(
        (thread, error) ->
            scenario.uncaught.add("%s escaped thread %s".formatted(error, thread.getName())));
    for (String override : overrides) {
      int eq = override.indexOf('=');
      scenario.properties.put(override.substring(0, eq), override.substring(eq + 1));
    }
    for (int i = 0; i < podCount; i++) {
      scenario.startPod("pod-" + (char) ('a' + i));
    }
    return scenario;
  }

  /** Starts one more pod (late join, takeover); extra overrides for this pod only. */
  public Pod startPod(String podName, String... overrides) {
    Map<String, String> props = new LinkedHashMap<>(properties);
    props.put("spring.application.name", "inv-" + name);
    props.put("spring.datasource.url", Infra.podJdbcUrl());
    props.put("spring.datasource.username", Infra.username());
    props.put("spring.datasource.password", Infra.password());
    props.put("it.pod", podName);
    for (String override : overrides) {
      int eq = override.indexOf('=');
      props.put(override.substring(0, eq), override.substring(eq + 1));
    }
    String[] args =
        props.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toArray(String[]::new);
    Pod pod = new Pod(podName, new SpringApplicationBuilder(PodApp.class).properties(args).run());
    pod.slots =
        Integer.parseInt(props.getOrDefault("opentmf.outbox.concurrent.max-in-flight", "8"));
    pods.add(pod);
    return pod;
  }

  /** A pod in a CHILD JVM - for real process death (it can be killed with SIGKILL). */
  public ChildPod startChildPod(String podName, String script, String... overrides) {
    Map<String, String> props = new LinkedHashMap<>(properties);
    props.put("spring.application.name", "inv-" + name);
    props.put("spring.datasource.url", Infra.podJdbcUrl());
    props.put("spring.datasource.username", Infra.username());
    props.put("spring.datasource.password", Infra.password());
    props.put("it.pod", podName);
    for (String override : overrides) {
      int eq = override.indexOf('=');
      props.put(override.substring(0, eq), override.substring(eq + 1));
    }
    ChildPod child = ChildPod.start(podName, script, props);
    children.add(child);
    return child;
  }

  public Pod pod(int index) {
    return pods.get(index);
  }

  public List<Pod> pods() {
    return pods;
  }

  public long seed() {
    return seed;
  }

  /** Marks a pod as killed: its unbooked deliveries explain duplicates. */
  void markKilled(String podName) {
    killed.add(podName);
  }

  /** SIGKILLs a child pod: its open transactions roll back, its leases just stop. */
  public void kill(ChildPod child) {
    markKilled(child.name);
    child.kill();
  }

  /**
   * Stops a pod gracefully (the grace stages run); what it could not book in its grace is
   * redelivered - its sends explain duplicates like a killed pod's.
   */
  public void stop(Pod pod) {
    markKilled(pod.name());
    pod.close();
  }

  /** Cuts every pod's database link for {@code duration}, then restores it. */
  public void outage(Duration duration) {
    Instant from = Instant.now();
    Infra.cutDatabase(true);
    try {
      Waits.interruptibly(duration);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    } finally {
      Infra.cutDatabase(false);
      outages.add(new Instant[] {from, Instant.now()});
    }
  }

  /** A note for the report (an observation, not a finding). */
  public void note(String text) {
    notes.add(text);
  }

  /** Counts rows by a WHERE clause, on the direct link. */
  public static long count(String where) {
    try (Connection c = Infra.connect();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("select count(*) from outbox where " + where)) {
      rs.next();
      return rs.getLong(1);
    } catch (SQLException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Inserts {@code rowsPerKey} CONCURRENT rows for each of {@code keys} keys straight into the
   * table, as an append would leave them (lane and key stamped; the destination is the key) - a
   * flood without ten thousand business transactions. Rows of one key get ascending ids.
   */
  public static void insertConcurrent(String destinationPrefix, int keys, int rowsPerKey) {
    sql(
        """
        insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
          created_on, attempts, next_attempt_on, lane, ordering_key)
        select 'it', 'k' || k, 'it.flood.v1', '%1$s' || k, '{}', now(), 0, now(), 'CONCURRENT',
          '%1$s' || k
        from generate_series(1, %2$d) r, generate_series(1, %3$d) k
        order by r, k\
        """
            .formatted(destinationPrefix, rowsPerKey, keys));
  }

  /** The first column of the first row of a query, as text (null when no row). */
  public static String sql1(String query) {
    try (Connection c = Infra.connect();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(query)) {
      return rs.next() ? rs.getString(1) : null;
    } catch (SQLException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /** Runs SQL on the direct link. */
  public static void sql(String statement) {
    try (Connection c = Infra.connect();
        Statement st = c.createStatement()) {
      st.execute(statement);
    } catch (SQLException ex) {
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Waits until nothing is open, sending or leased (or the timeout), then checks every invariant;
   * returns the findings - empty when all hold.
   */
  public Findings finish(Duration quiescence, Expect expect) {
    Findings findings = new Findings(name, seed);
    try {
      await()
          .pollInterval(Duration.ofMillis(200))
          .atMost(quiescence)
          .until(
              () ->
                  count("relayed_on is null and cancelled_on is null and parked_on is null") == 0
                      && Receiver.sending().isEmpty()
                      && count("claimed_until > now() and relayed_on is null") == 0);
    } catch (ConditionTimeoutException ex) {
      findings.add(
          "Q",
          "not quiescent within %s: %d open, %d sending, %d leased"
              .formatted(
                  quiescence,
                  count("relayed_on is null and cancelled_on is null and parked_on is null"),
                  Receiver.sending().size(),
                  count("claimed_until > now() and relayed_on is null")));
    }
    Invariants.check(this, expect, findings);
    findings.notes.addAll(notes);
    report(findings);
    return findings;
  }

  private void report(Findings findings) {
    Path file = Path.of("target", "invariants-report.tsv");
    try {
      Files.createDirectories(file.getParent());
      if (!Files.exists(file)) {
        Files.writeString(file, "scenario\tseed\tresult\tfindings\tfigures\tnotes\n");
      }
      Files.writeString(
          file,
          "%s\t%d\t%s\t%s\t%s\t%s%n"
              .formatted(
                  name,
                  seed,
                  findings.isEmpty() ? "PASS" : "FAIL",
                  String.join(" | ", findings.items),
                  String.join(", ", findings.figures),
                  String.join(" | ", findings.notes)),
          StandardOpenOption.APPEND);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    System.out.println(findings);
  }

  @Override
  public void close() {
    Receiver.release();
    for (ChildPod child : children) {
      child.kill();
    }
    for (Pod pod : pods) {
      try {
        pod.close();
      } catch (RuntimeException ex) {
        System.out.println("INVARIANTS: closing " + pod + " failed: " + ex);
      }
    }
    Infra.healDatabase();
    Thread.setDefaultUncaughtExceptionHandler(previousHandler);
  }
}
