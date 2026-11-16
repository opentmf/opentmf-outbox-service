package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import liquibase.command.CommandScope;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * OTMF-OUTBOX-PRUNE-STATEMENT-BOUNDED-AND-PLANNED-1: the retention prune's batch statements ({@link
 * OutboxPruneSql}, the very texts the repository runs, PREPAREd as the JDBC driver sends them) on a
 * REALISTIC analysed backlog: 200,000 rows booked open and then relayed or cancelled by UPDATE in a
 * shuffled order, as relays and cancels do - so the new row versions lie in update order, not in
 * the order of their stamps (correlation about 0), and about half of them are past the retention.
 * Under both a custom and a generic plan the batch takes its rows from {@code ix_outbox_relayed_on}
 * / {@code ix_outbox_cancelled_on}, never from a scan of the table: in index order when the batch
 * is a small share of the expired rows, and where it is a large share the planner may read just
 * those rows through the index and sort them in memory - bounded by the expired rows either way.
 *
 * <p>{@code plan_cache_mode} is set on this test's OWN connection, as a measuring device; the
 * library never sets a plan mode. The plan needs the table's statistics: on a table never analysed
 * (autovacuum off, or within about a minute of a bulk load or a logical restore) PostgreSQL's
 * custom plan sorts the whole expired backlog - measured 0.12 s at 10,000 rows to 2.7 s at 700,000,
 * README "The retention prune".
 */
@Testcontainers
class OutboxPrunePlanIT {

  @Container
  static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.1-alpine3.22");

  private static final int ROWS = 200_000;
  private static final List<String> PLAN_MODES = List.of("force_custom_plan", "force_generic_plan");

  @BeforeAll
  static void aRealisticBacklog() throws Exception {
    new CommandScope("update")
        .addArgumentValue("url", postgres.getJdbcUrl())
        .addArgumentValue("username", postgres.getUsername())
        .addArgumentValue("password", postgres.getPassword())
        .addArgumentValue("changelogFile", "db/changelog/opentmf-outbox.sql")
        .execute();
    execute(
        // booked open over the last 14 days: created_on in booking order
        """
        insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
          created_on, attempts, next_attempt_on)
        select 't', 'a' || g, 'e', 'https://hub/' || (g %% 50), '{}',
          now() - interval '14 days' + g * (interval '14 days' / %d), 0, now()
        from generate_series(1, %d) g\
        """
            .formatted(ROWS, ROWS),
        // relayed (nine in ten) or cancelled (one in ten) by UPDATE, 100 rows at a time in a
        // shuffled order: each new row version lands where the update put it
        """
        do $$ declare b int; begin
          for b in select x from generate_series(0, %d) x order by random() loop
            update outbox set relayed_on = created_on + random() * interval '5 seconds',
              attempts = 1
            where id > b * 100 and id <= (b + 1) * 100 and id %% 10 <> 0;
            update outbox set cancelled_on = created_on + random() * interval '5 seconds'
            where id > b * 100 and id <= (b + 1) * 100 and id %% 10 = 0;
          end loop;
        end $$\
        """
            .formatted(ROWS / 100 - 1),
        // and today's open rows
        """
        insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
          created_on, attempts, next_attempt_on)
        select 't', 'o' || g, 'e', 'https://hub/1', '{}', now(), 0, now()
        from generate_series(1, 2000) g\
        """,
        "vacuum analyze outbox");
  }

  private static Connection connect() throws SQLException {
    return DriverManager.getConnection(
        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  private static void execute(String... sql) throws SQLException {
    try (Connection c = connect();
        Statement st = c.createStatement()) {
      for (String s : sql) {
        st.execute(s);
      }
    }
  }

  private static double correlation(String column) throws SQLException {
    try (Connection c = connect();
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "select correlation from pg_stats where tablename = 'outbox' and attname = '"
                    + column
                    + "'")) {
      rs.next();
      return rs.getDouble(1);
    }
  }

  /**
   * EXPLAIN (ANALYZE, BUFFERS) of one prune batch: the statement PREPAREd with {@code $1} the
   * cutoff and {@code $2} the limit, executed with the retention's cutoff (7 days) and the default
   * batch, under the given plan mode - this session only - and rolled back.
   */
  private static JsonNode explain(String statement, String planMode) throws Exception {
    String sql = statement.replace(":cutoff", "$1").replace(":limit", "$2");
    try (Connection c = connect();
        Statement st = c.createStatement()) {
      c.setAutoCommit(false);
      try {
        st.execute("set plan_cache_mode = " + planMode); // the measuring device, this session only
        st.execute("prepare prune (timestamptz, int) as " + sql);
        try (ResultSet rs =
            st.executeQuery(
                "explain (analyze, buffers, format json)"
                    + " execute prune (now() - interval '7 days', 5000)")) {
          rs.next();
          return new ObjectMapper().readTree(rs.getString(1)).get(0);
        }
      } finally {
        c.rollback();
      }
    }
  }

  private static List<JsonNode> nodes(JsonNode node, List<JsonNode> into) {
    into.add(node);
    for (JsonNode child : node.path("Plans")) {
      nodes(child, into);
    }
    return into;
  }

  private static long count(String where) throws SQLException {
    try (Connection c = connect();
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("select count(*) from outbox where " + where)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  /**
   * One batch under both plan modes: its rows come from {@code index} - an index scan in stamp
   * order, or a bitmap scan of the index's expired entries - never from a scan of the table; a
   * Sort, where the planner chooses one, sorts only the expired rows the index found, in memory.
   *
   * @return whether either plan sorted
   */
  private static boolean assertBatchFromTheIndex(
      String what, String statement, String index, String stamp) throws Exception {
    long expired = count(stamp + " < now() - interval '7 days'");
    boolean sorted = false;
    for (String mode : PLAN_MODES) {
      JsonNode explained = explain(statement, mode);
      List<JsonNode> all = nodes(explained.get("Plan"), new ArrayList<>());
      assertThat(all)
          .as("%s, %s: no sequential scan of outbox", what, mode)
          .noneMatch(n -> n.get("Node Type").asString().equals("Seq Scan"));
      assertThat(all)
          .as("%s, %s: the batch's rows come from %s", what, mode, index)
          .anyMatch(
              n ->
                  n.get("Node Type").asString().endsWith("Index Scan")
                      && index.equals(n.path("Index Name").asString(null)));
      for (JsonNode sort : all) {
        if (sort.get("Node Type").asString().equals("Sort")) {
          sorted = true;
          assertThat(sort.get("Plans").get(0).get("Actual Rows").asDouble())
              .as("%s, %s: the sort takes only the %d expired rows", what, mode, expired)
              .isLessThanOrEqualTo(expired);
          assertThat(sort.get("Sort Space Type").asString())
              .as("%s, %s: in memory", what, mode)
              .isEqualTo("Memory");
        }
      }
      JsonNode plan = explained.get("Plan");
      System.out.printf(
          "PRUNE-PLAN-FIGURES: %s (%d expired) | %s %d buffers %.2f ms%s%n",
          what,
          expired,
          mode,
          plan.get("Shared Hit Blocks").asLong() + plan.get("Shared Read Blocks").asLong(),
          explained.get("Execution Time").asDouble(),
          all.stream().anyMatch(n -> n.get("Node Type").asString().equals("Sort"))
              ? " (sorted)"
              : "");
    }
    return sorted;
  }

  @Test
  void theBacklogIsRealistic_itsStampsAreNotInPhysicalOrder() throws Exception {
    assertThat(Math.abs(correlation("relayed_on"))).isLessThan(0.2);
    assertThat(Math.abs(correlation("cancelled_on"))).isLessThan(0.2);
  }

  /** About 90,000 expired relayed rows against a batch of 5,000: taken in index order, no Sort. */
  @Test
  void aRelayedBatch_takesItsRowsFromTheIndex_inIndexOrder_underBothPlanModes() throws Exception {
    assertThat(
            assertBatchFromTheIndex(
                "prune relayed batch",
                OutboxPruneSql.RELAYED,
                "ix_outbox_relayed_on",
                "relayed_on"))
        .as("no Sort node: the batch is a small share of the expired rows")
        .isFalse();
  }

  /**
   * About 10,000 expired cancelled rows against a batch of 5,000: the batch is half of them, and
   * the custom plan reads them all through the index's bitmap and sorts them in memory (measured
   * 115 ms; with a batch of 1,000 it walks the index) - bounded by the expired cancelled rows,
   * never by the table.
   */
  @Test
  void aCancelledBatch_takesItsRowsFromTheIndex_underBothPlanModes() throws Exception {
    assertBatchFromTheIndex(
        "prune cancelled batch",
        OutboxPruneSql.CANCELLED,
        "ix_outbox_cancelled_on",
        "cancelled_on");
  }
}
