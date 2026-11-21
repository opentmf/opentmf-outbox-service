package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import liquibase.command.CommandScope;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * A service's lookup of an aggregate's rows on a loaded table (1.11.1, changeset 010,
 * OUTBOX-THE-LOOKUP-BY-AGGREGATE-HAS-A-PLAN-ON-A-LOADED-TABLE-1): the newest row of an aggregate,
 * and its newest row above a given id, over 1,000,000 rows of a seven-day mix - three rows per
 * aggregate as a service writes them, nearly all relayed. Up to 1.11.0 the planner answered both by
 * walking the primary key back from the newest row and filtering every row it met: about 250 ms at
 * this size, and every row of the table for an aggregate not written yet (the intake's case).
 *
 * <p>The server's own plans only: a first execution and a PREPAREd statement after six executions
 * (where a driver's plan goes generic if the server so chooses) - no {@code plan_cache_mode}, no
 * hint. And the index must not move the relay: the claim, re-check, stamp, heartbeat, give-back and
 * booking statements - the very texts of {@link OutboxClaimSql} - plan the same with and without
 * it.
 */
@Testcontainers
class OutboxAggregateLookupPlanIT {

  @Container
  static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.1-alpine3.22");

  private static final int ROWS = 1_000_000;

  /** Measured 2-5 at 100,000 to 5,000,000 rows; the primary-key walk read 86,000 at 1,000,000. */
  private static final long BUFFER_BOUND = 20;

  private static final String NEWEST =
      "select * from outbox where aggregate_type = $1 and aggregate_id = $2"
          + " order by id desc limit 1";

  private static final String NEWEST_ABOVE =
      "select * from outbox where aggregate_type = $1 and aggregate_id = $2 and id > $3"
          + " order by id desc limit 1";

  private static final Pattern NAMED = Pattern.compile("(?<!:):(\\w+)");

  private static final Map<String, String> TYPES =
      Map.ofEntries(
          Map.entry("now", "timestamptz"),
          Map.entry("after", "varchar"),
          Map.entry("until", "varchar"),
          Map.entry("limit", "int"),
          Map.entry("namespace", "int"),
          Map.entry("keys", "varchar"),
          Map.entry("ids", "varchar"),
          Map.entry("id", "bigint"),
          Map.entry("stamp", "timestamptz"),
          Map.entry("relayedOn", "timestamptz"),
          Map.entry("stamps", "varchar"),
          Map.entry("renewed", "varchar"),
          Map.entry("since", "varchar"));

  private static String oldType;
  private static String oldId;
  private static long oldFirstRow;
  private static String recentType;
  private static String recentId;

  @BeforeAll
  static void aWeekOfRows() throws Exception {
    new CommandScope("update")
        .addArgumentValue("url", postgres.getJdbcUrl())
        .addArgumentValue("username", postgres.getUsername())
        .addArgumentValue("password", postgres.getPassword())
        .addArgumentValue("changelogFile", "db/changelog/opentmf-outbox.sql")
        .execute();
    try (Connection c = connect();
        Statement st = c.createStatement()) {
      // id order = time order over seven days; three adjacent rows per aggregate; the newest
      // 2,000 rows open, half of the aggregates on each lane.
      st.execute(
          """
          insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
            created_on, attempts, next_attempt_on, relayed_on, lane, ordering_key)
          select (array['communication-message', 'party-interaction', 'document'])[1 + (g / 3) % 3],
            md5('aggregate-' || (g / 3)), 'flow.v1',
            case when (g / 3) % 2 = 0 then 'http://receiver/notify' else 'flow.events' end,
            rpad(md5(g::text), 300, 'x'),
            now() - interval '7 days' + interval '7 days' * g / {rows}, 0,
            now() - interval '7 days' + interval '7 days' * g / {rows},
            case when g <= {rows} - 2000 then now() - interval '7 days' + interval '7 days' * g / {rows} end,
            case when (g / 3) % 2 = 0 then 'CONCURRENT' else 'ORDERED' end,
            case when (g / 3) % 2 = 0 then md5('aggregate-' || (g / 3)) end
          from generate_series(1, {rows}) g order by g\
          """
              .replace("{rows}", String.valueOf(ROWS)));
      st.execute("vacuum analyze outbox");
      try (ResultSet rs =
          st.executeQuery(
              "select aggregate_type, aggregate_id, id from outbox where id = "
                  + (ROWS / 100 + 1))) {
        rs.next();
        oldType = rs.getString(1);
        oldId = rs.getString(2);
        oldFirstRow = rs.getLong(3);
      }
      try (ResultSet rs =
          st.executeQuery(
              "select aggregate_type, aggregate_id from outbox where id = " + (ROWS - ROWS / 50))) {
        rs.next();
        recentType = rs.getString(1);
        recentId = rs.getString(2);
      }
    }
  }

  private static Connection connect() throws SQLException {
    return DriverManager.getConnection(
        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  private static List<JsonNode> nodes(JsonNode node, List<JsonNode> into) {
    into.add(node);
    JsonNode children = node.get("Plans");
    if (children != null) {
      children.forEach(child -> nodes(child, into));
    }
    return into;
  }

  private static JsonNode json(ResultSet rs) throws SQLException {
    rs.next();
    return new ObjectMapper().readTree(rs.getString(1)).get(0);
  }

  /**
   * The lookup's plans as the server chooses them: the first execution of the PREPAREd statement,
   * and the one after six executions.
   */
  private static List<JsonNode> lookupPlans(String sql, String types, String values)
      throws SQLException {
    try (Connection c = connect();
        Statement st = c.createStatement()) {
      st.execute("prepare lookup (" + types + ") as " + sql);
      List<JsonNode> plans = new ArrayList<>();
      try (ResultSet rs =
          st.executeQuery(
              "explain (analyze, buffers, format json) execute lookup (" + values + ")")) {
        plans.add(json(rs));
      }
      for (int i = 0; i < 6; i++) {
        st.execute("execute lookup (" + values + ")");
      }
      try (ResultSet rs =
          st.executeQuery(
              "explain (analyze, buffers, format json) execute lookup (" + values + ")")) {
        plans.add(json(rs));
      }
      return plans;
    }
  }

  private static void assertTheIndexServes(String what, List<JsonNode> plans) {
    for (int i = 0; i < plans.size(); i++) {
      String run = what + (i == 0 ? ", first execution" : ", after six executions");
      JsonNode plan = plans.get(i).get("Plan");
      List<JsonNode> all = nodes(plan, new ArrayList<>());
      long buffers =
          plan.get("Shared Hit Blocks").asLong() + plan.get("Shared Read Blocks").asLong();
      System.out.printf(
          "PLAN-FIGURES: %s | %d buffers %.3f ms%n",
          run, buffers, plans.get(i).get("Execution Time").asDouble());
      assertThat(all)
          .as("%s: no sequential scan of the table", run)
          .noneMatch(n -> n.get("Node Type").asString().equals("Seq Scan"));
      assertThat(all)
          .as("%s: no walk of the primary key", run)
          .noneMatch(n -> "outbox_pkey".equals(n.path("Index Name").asString(null)));
      assertThat(all)
          .as("%s: the aggregate's index", run)
          .anyMatch(n -> "ix_outbox_aggregate".equals(n.path("Index Name").asString(null)));
      assertThat(buffers).as("%s: buffers", run).isLessThanOrEqualTo(BUFFER_BOUND);
    }
  }

  private static String quoted(String text) {
    return "'" + text + "'";
  }

  @Test
  void theNewestRowOfAnAggregate_isReadFromTheIndex_whateverItsAge() throws Exception {
    assertTheIndexServes(
        "newest, an aggregate not written yet",
        lookupPlans(
            NEWEST,
            "varchar, varchar",
            quoted("communication-message") + ", " + quoted("never-written")));
    assertTheIndexServes(
        "newest, an aggregate of day one",
        lookupPlans(NEWEST, "varchar, varchar", quoted(oldType) + ", " + quoted(oldId)));
    assertTheIndexServes(
        "newest, a recent aggregate",
        lookupPlans(NEWEST, "varchar, varchar", quoted(recentType) + ", " + quoted(recentId)));
  }

  @Test
  void theNewestRowAboveAnId_isReadFromTheIndex_whateverTheBound() throws Exception {
    assertTheIndexServes(
        "above, an aggregate of day one, bound below its first row",
        lookupPlans(
            NEWEST_ABOVE,
            "varchar, varchar, bigint",
            quoted(oldType) + ", " + quoted(oldId) + ", " + (oldFirstRow - 1)));
    assertTheIndexServes(
        "above, an aggregate not written yet, bound at the table's start",
        lookupPlans(
            NEWEST_ABOVE,
            "varchar, varchar, bigint",
            quoted("communication-message") + ", " + quoted("never-written") + ", 0"));
  }

  // ---------------------------------------------------------------- the relay's plans unchanged

  /** A plan's shape: every node's type, relation and index, depth-first. */
  private static String shape(JsonNode node) {
    StringBuilder out = new StringBuilder();
    for (JsonNode n : nodes(node, new ArrayList<>())) {
      out.append(n.get("Node Type").asString())
          .append('/')
          .append(n.path("Relation Name").asString(""))
          .append('/')
          .append(n.path("Index Name").asString(""))
          .append(';');
    }
    return out.toString();
  }

  /** EXPLAIN of one {@link OutboxClaimSql} statement, PREPAREd with its named parameters. */
  private static String planShape(
      Statement st, String name, String statement, Map<String, String> args) throws SQLException {
    List<String> names = new ArrayList<>();
    Matcher named = NAMED.matcher(statement);
    StringBuilder sql = new StringBuilder();
    while (named.find()) {
      String param = named.group(1);
      if (!names.contains(param)) {
        names.add(param);
      }
      named.appendReplacement(sql, "\\$" + (names.indexOf(param) + 1));
    }
    named.appendTail(sql);
    String types = String.join(", ", names.stream().map(TYPES::get).toList());
    String values = String.join(", ", names.stream().map(args::get).toList());
    st.execute("prepare " + name + " (" + types + ") as " + sql);
    try (ResultSet rs =
        st.executeQuery("explain (format json) execute " + name + " (" + values + ")")) {
      String shape = shape(json(rs).get("Plan"));
      st.execute("deallocate " + name);
      return shape;
    }
  }

  private static Map<String, String> relayPlans(Statement st, String ids, String stamps, long id)
      throws SQLException {
    Map<String, String> plans = new LinkedHashMap<>();
    plans.put(
        "ORDERED",
        planShape(st, "ordered", OutboxClaimSql.ORDERED, Map.of("now", "now()", "limit", "100")));
    plans.put(
        "CONCURRENT",
        planShape(
            st,
            "concurrent",
            OutboxClaimSql.CONCURRENT,
            Map.of("now", "now()", "after", "''", "limit", "100")));
    plans.put(
        "CONCURRENT_HEADS",
        planShape(
            st,
            "heads",
            OutboxClaimSql.CONCURRENT_HEADS,
            Map.of("now", "now()", "after", "''", "limit", "100")));
    plans.put(
        "CONCURRENT_WRAP",
        planShape(
            st,
            "wrap",
            OutboxClaimSql.CONCURRENT_WRAP,
            Map.of("now", "now()", "until", "'~'", "limit", "100")));
    plans.put(
        "RECHECK",
        planShape(
            st, "recheck", OutboxClaimSql.RECHECK, Map.of("now", "now()", "ids", quoted(ids))));
    plans.put(
        "STAMP",
        planShape(st, "stamp", OutboxClaimSql.STAMP, Map.of("stamp", "now()", "ids", quoted(ids))));
    plans.put(
        "RENEW_SENDING",
        planShape(
            st,
            "renew",
            OutboxClaimSql.RENEW_SENDING,
            Map.of(
                "ids", quoted(ids),
                "stamps", quoted(stamps),
                "renewed", quoted(stamps),
                "since", quoted(stamps))));
    plans.put(
        "RELEASE",
        planShape(
            st,
            "release",
            OutboxClaimSql.RELEASE,
            Map.of("ids", quoted(ids), "stamps", quoted(stamps), "now", "now()")));
    plans.put(
        "BOOK_RELAYED",
        planShape(
            st,
            "book",
            OutboxClaimSql.BOOK_RELAYED,
            Map.of("id", String.valueOf(id), "stamp", "now()", "relayedOn", "now()")));
    return plans;
  }

  @Test
  void theRelaysClaimAndBookingStatements_planAlike_withAndWithoutTheIndex() throws Exception {
    try (Connection c = connect();
        Statement st = c.createStatement()) {
      String ids;
      String stamps;
      long id;
      try (ResultSet rs =
          st.executeQuery(
              "select '{' || string_agg(id::text, ',') || '}', '{' || string_agg('2100-01-01"
                  + " 00:00:00+00', ',') || '}', min(id) from (select id from outbox where"
                  + " relayed_on is null order by id limit 100) h")) {
        rs.next();
        ids = rs.getString(1);
        stamps = rs.getString(2);
        id = rs.getLong(3);
      }
      Map<String, String> withIndex = relayPlans(st, ids, stamps, id);
      c.setAutoCommit(false);
      try {
        st.execute("drop index ix_outbox_aggregate"); // this transaction only: rolled back below
        Map<String, String> withoutIndex = relayPlans(st, ids, stamps, id);
        assertThat(withIndex).isEqualTo(withoutIndex);
        assertThat(String.join("", withIndex.values())).doesNotContain("ix_outbox_aggregate");
      } finally {
        c.rollback();
      }
    }
  }
}
