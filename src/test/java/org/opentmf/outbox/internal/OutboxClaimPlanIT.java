package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import liquibase.command.CommandScope;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * The CONCURRENT claim's COST, pinned on the real planner: {@code EXPLAIN (ANALYZE, BUFFERS)} of
 * {@link OutboxClaimSql#CONCURRENT} and {@link OutboxClaimSql#CONCURRENT_WRAP} - the very texts
 * the repository runs, PREPAREd - over a table carrying 1,000,000 relayed rows, every case under
 * BOTH a custom and a generic plan ({@code plan_cache_mode} set on this test's own connection as a
 * measuring device - the library never sets it). In every case the claim must not read the
 * relayed part of the table (no sequential scan, no primary-key walk beyond the rows it claims),
 * its key skip-scan must filter no rows (the key range is an index condition, never a filter),
 * and it must touch a small, FIXED number of buffers - whatever the size of the table, of the
 * lane's backlog or of one key's backlog, in flight or not, above or below the cursor.
 */
@Testcontainers
@TestMethodOrder(OrderAnnotation.class)
class OutboxClaimPlanIT {

  @Container
  static final PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:18.1-alpine3.22");

  /** The claim's slot count in every case (the default {@code max-in-flight}). */
  private static final int LIMIT = 8;

  /** Measured 2-131 at 5M relayed rows; the 1.3.0 first cut read 508,427. */
  private static final long BUFFER_BOUND = 400;

  private static final Pattern NAMED = Pattern.compile("(?<!:):(\\w+)");

  /** The parameter types of the claim statements, by name. */
  private static final Map<String, String> TYPES =
      Map.of(
          "now", "timestamptz",
          "after", "varchar",
          "until", "varchar",
          "limit", "int",
          "namespace", "int",
          "key", "varchar",
          "ids", "varchar");

  private static final List<String> PLAN_MODES = List.of("force_custom_plan", "force_generic_plan");

  @BeforeAll
  static void schemaAndRelayedHistory() throws Exception {
    new CommandScope("update")
        .addArgumentValue("url", postgres.getJdbcUrl())
        .addArgumentValue("username", postgres.getUsername())
        .addArgumentValue("password", postgres.getPassword())
        .addArgumentValue("changelogFile", "db/changelog/opentmf-outbox.sql")
        .execute();
    execute(
        """
        insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
          created_on, attempts, next_attempt_on, relayed_on, lane, ordering_key)
        select 't', 'a', 'e', 'https://hub/' || (g % 50), '{}', now() - interval '1 day', 0,
          now() - interval '1 day', now() - interval '1 day',
          case when g % 3 = 0 then 'CONCURRENT' else 'ORDERED' end,
          case when g % 3 = 0 then 'https://hub/' || (g % 50) end
        from generate_series(1, 1000000) g""");
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
      st.execute("vacuum analyze outbox");
    }
  }

  /**
   * EXPLAIN (ANALYZE, BUFFERS) of one claim statement, PREPAREd with its named parameters as
   * {@code $n}, executed with the given argument expressions under the given plan mode, rolled
   * back (ANALYZE runs the FOR UPDATE).
   */
  private static JsonNode explain(String statement, String planMode, Map<String, String> args)
      throws Exception {
    List<String> names = new ArrayList<>();
    Matcher named = NAMED.matcher(statement);
    StringBuilder sql = new StringBuilder();
    while (named.find()) {
      String name = named.group(1);
      if (!names.contains(name)) {
        names.add(name);
      }
      named.appendReplacement(sql, "\\$" + (names.indexOf(name) + 1));
    }
    named.appendTail(sql);
    String types = String.join(", ", names.stream().map(TYPES::get).toList());
    String values = String.join(", ", names.stream().map(args::get).toList());
    try (Connection c = connect();
        Statement st = c.createStatement()) {
      c.setAutoCommit(false);
      try {
        st.execute("set plan_cache_mode = " + planMode); // the measuring device, this session only
        st.execute("prepare claim (" + types + ") as " + sql);
        try (ResultSet rs =
            st.executeQuery(
                "explain (analyze, buffers, format json) execute claim (" + values + ")")) {
          rs.next();
          return new ObjectMapper().readTree(rs.getString(1)).get(0);
        }
      } finally {
        c.rollback();
      }
    }
  }

  private static JsonNode explainClaim(String planMode, String after) throws Exception {
    return explain(
        OutboxClaimSql.CONCURRENT,
        planMode,
        Map.of("now", "now()", "after", "'" + after + "'", "limit", String.valueOf(LIMIT)));
  }

  private static JsonNode explainWrap(String planMode, String until) throws Exception {
    return explain(
        OutboxClaimSql.CONCURRENT_WRAP,
        planMode,
        Map.of("now", "now()", "until", "'" + until + "'", "limit", String.valueOf(LIMIT)));
  }

  /** The claim of both statements' cases under both plan modes: rows claimed, per mode. */
  private static List<Long> claimedUnderBothModes(Callable<String, JsonNode> claim)
      throws Exception {
    List<Long> claimed = new ArrayList<>();
    for (String mode : PLAN_MODES) {
      claimed.add(assertBoundedClaim(mode, claim.call(mode)));
    }
    return claimed;
  }

  /** A claim under one plan mode. */
  @FunctionalInterface
  interface Callable<A, R> {
    R call(A argument) throws Exception;
  }

  /** Every plan node, depth first. */
  private static List<JsonNode> nodes(JsonNode node, List<JsonNode> into) {
    into.add(node);
    JsonNode children = node.get("Plans");
    if (children != null) {
      children.forEach(child -> nodes(child, into));
    }
    return into;
  }

  /**
   * The shape every case must keep: no sequential scan of the table, the primary key used only
   * to fetch the rows claimed, and a fixed, small buffer count. Returns the claimed row count.
   */
  private static long assertBoundedClaim(String planMode, JsonNode explained) {
    JsonNode plan = explained.get("Plan");
    List<JsonNode> all = nodes(plan, new ArrayList<>());
    // the key range is an INDEX CONDITION under either plan mode, never a filter over a backlog
    assertThat(all)
        .as("%s: rows filtered on the key skip-scan", planMode)
        .filteredOn(n -> "ix_outbox_concurrent_keyed".equals(n.path("Index Name").asString(null)))
        .allSatisfy(
            n ->
                assertThat(n.path("Rows Removed by Filter").asLong(0))
                    .isLessThanOrEqualTo(LIMIT));
    assertThat(all)
        .noneMatch(
            n ->
                n.get("Node Type").asString().equals("Seq Scan")
                    && "outbox".equals(n.path("Relation Name").asString(null)));
    assertThat(all)
        .filteredOn(n -> "outbox_pkey".equals(n.path("Index Name").asString(null)))
        .allSatisfy(
            n -> {
              assertThat(n.get("Actual Loops").asLong()).isLessThanOrEqualTo(LIMIT);
              // the locked row is re-checked against the WHOLE eligibility predicate
              assertThat(n.path("Filter").asString(""))
                  .contains("next_attempt_on <=")
                  .contains("release_at <=")
                  .contains("claimed_until <=")
                  .contains("parked_on IS NULL");
            });
    long buffers = plan.get("Shared Hit Blocks").asLong() + plan.get("Shared Read Blocks").asLong();
    assertThat(buffers)
        .as("%s: buffers read by one claim", planMode)
        .isLessThanOrEqualTo(BUFFER_BOUND);
    return plan.get("Actual Rows").asLong();
  }

  @Test
  @Order(1)
  void idle_aMillionRelayedRows_noneClaimable() throws Exception {
    assertThat(claimedUnderBothModes(mode -> explainClaim(mode, ""))).containsOnly(0L);
  }

  @Test
  @Order(2)
  void aHundredThousandRowsOfOneKey_whoseFirstRowIsInFlight() throws Exception {
    execute(
        """
        insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
          created_on, attempts, next_attempt_on, lane, ordering_key)
        select 't', 'a', 'e', 'hub:1', '{}', now(), 0, now() - interval '1 second',
          'CONCURRENT', 'hub:1'
        from generate_series(1, 100000)""",
        """
        update outbox set claimed_until = now() + interval '2 minutes',
          next_attempt_on = now() + interval '2 minutes'
        where id = (select min(id) from outbox where ordering_key = 'hub:1'
          and relayed_on is null)""");

    assertThat(claimedUnderBothModes(mode -> explainClaim(mode, ""))).containsOnly(0L); // busy
  }

  @Test
  @Order(3)
  void theSameBacklog_withItsKeyNotInFlight_claimsExactlyItsHead() throws Exception {
    execute(
        """
        update outbox set claimed_until = null, next_attempt_on = now() - interval '1 second'
        where ordering_key = 'hub:1' and relayed_on is null and claimed_until is not null""");

    assertThat(claimedUnderBothModes(mode -> explainClaim(mode, ""))).containsOnly(1L);
  }

  /**
   * The wrap pass with a large backlog ABOVE the cursor: keys up to "hub:0" while 100,000 rows
   * of "hub:1" are pending - the bound is an index condition, so not one of them is read.
   */
  @Test
  @Order(4)
  void theWrapPass_neverReadsTheBacklogAboveTheCursor() throws Exception {
    assertThat(claimedUnderBothModes(mode -> explainWrap(mode, "hub:0"))).containsOnly(0L);
  }

  @Test
  @Order(5)
  void tenThousandKeysOfTenRows_fromAnyCursor() throws Exception {
    execute(
        "delete from outbox where relayed_on is null",
        """
        insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
          created_on, attempts, next_attempt_on, lane, ordering_key)
        select 't', 'a', 'e', 'hub:k' || (g % 10000), '{}', now(), 0,
          now() - interval '1 second', 'CONCURRENT', 'hub:k' || (g % 10000)
        from generate_series(1, 100000) g""");

    assertThat(claimedUnderBothModes(mode -> explainClaim(mode, ""))).containsOnly(8L);
    assertThat(claimedUnderBothModes(mode -> explainClaim(mode, "hub:k5000"))).containsOnly(8L);
    // the wrap: keys up to the cursor (string order - "hub:k1000" would bound it to five keys)
    assertThat(claimedUnderBothModes(mode -> explainWrap(mode, "hub:k5"))).containsOnly(8L);
  }

  // ------------------------------------------------------------ the cross-pod key lock

  /**
   * One statement under both plan modes: no sequential scan of {@code outbox}, and every index
   * it reads is one of {@code allowed}. Returns the generic run's buffers and time, for the record.
   */
  private static String assertIndexedOnly(
      String what, String statement, Map<String, String> args, List<String> allowed)
      throws Exception {
    StringBuilder figures = new StringBuilder(what);
    for (String mode : PLAN_MODES) {
      JsonNode explained = explain(statement, mode, args);
      List<JsonNode> all = nodes(explained.get("Plan"), new ArrayList<>());
      assertThat(all)
          .as("%s, %s: no sequential scan of outbox", what, mode)
          .noneMatch(
              n ->
                  n.get("Node Type").asString().equals("Seq Scan")
                      && "outbox".equals(n.path("Relation Name").asString(null)));
      assertThat(all)
          .as("%s, %s: indexes read", what, mode)
          .filteredOn(n -> n.has("Index Name"))
          .extracting(n -> n.get("Index Name").asString())
          .isNotEmpty()
          .allMatch(allowed::contains);
      JsonNode plan = explained.get("Plan");
      figures.append(
          " | %s %d buffers %.2f ms"
              .formatted(
                  mode.substring(6, 13),
                  plan.get("Shared Hit Blocks").asLong()
                      + plan.get("Shared Read Blocks").asLong(),
                  explained.get("Execution Time").asDouble()));
    }
    System.out.println("PLAN-FIGURES: " + figures); // the PR's evidence
    return figures.toString();
  }

  /**
   * The cross-pod guard's two extra statements, on the 10,000-key data with one key's head in
   * flight: the RE-CHECK of a full claim's candidates reads the primary key and the two key-side
   * indexes - never a sequential scan - under both plan modes; the KEY LOCK is a function call
   * that reads nothing. Their cost per claim is printed (the claim statements themselves are
   * unchanged, so their pins above stand as they are).
   */
  @Test
  @Order(7)
  void theCrossPodGuard_readsByIndexOnly_andCostsLittle() throws Exception {
    execute(
        """
        update outbox set claimed_until = now() + interval '2 minutes',
          next_attempt_on = now() + interval '2 minutes'
        where id = (select min(id) from outbox where ordering_key = 'hub:k1'
          and relayed_on is null)""");
    String ids;
    try (Connection c = connect();
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "select '{' || string_agg(id::text, ',') || '}' from (select min(id) id"
                    + " from outbox where relayed_on is null and lane = 'CONCURRENT'"
                    + " group by ordering_key order by ordering_key limit 8) h")) {
      rs.next();
      ids = rs.getString(1);
    }
    assertIndexedOnly(
        "cross-pod re-check of 8 candidates",
        OutboxClaimSql.RECHECK,
        Map.of("now", "now()", "ids", "'" + ids + "'"),
        List.of("outbox_pkey", "ix_outbox_claimed_until", "ix_outbox_concurrent_keyed"));
    for (String mode : PLAN_MODES) {
      JsonNode lock =
          explain(
              OutboxClaimSql.KEY_LOCK,
              mode,
              Map.of(
                  "namespace", String.valueOf(OutboxClaimSql.KEY_LOCK_NAMESPACE),
                  "key", "'hub:k2'"));
      JsonNode plan = lock.get("Plan");
      assertThat(nodes(plan, new ArrayList<>()))
          .noneMatch(n -> "outbox".equals(n.path("Relation Name").asString(null)));
      System.out.println(
          "PLAN-FIGURES: key lock | %s %d buffers %.3f ms"
              .formatted(
                  mode,
                  plan.get("Shared Hit Blocks").asLong() + plan.get("Shared Read Blocks").asLong(),
                  lock.get("Execution Time").asDouble()));
    }
  }
}
