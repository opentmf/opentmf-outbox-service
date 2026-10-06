package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
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
 * {@link OutboxClaimSql#CONCURRENT} - the very text the repository runs - over a table carrying
 * 1,000,000 relayed rows. In every case the claim must not read the relayed part of the table
 * (no sequential scan, no primary-key walk beyond the rows it claims) and must touch a small,
 * FIXED number of buffers - whatever the size of the table, of the lane's backlog or of one key's
 * backlog, and whether that key is in flight or not. (Reported for 5,000,000 relayed rows in
 * PR #6; this test keeps the shape from regressing.)
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

  /** EXPLAIN (ANALYZE, BUFFERS) of the claim, its named parameters bound, rolled back. */
  private static JsonNode explainClaim(String after, String until) throws Exception {
    Map<String, Object> values =
        Map.of(
            "now", Timestamp.from(Instant.now()),
            "after", after,
            "limit", LIMIT,
            "unkeyedLimit", LIMIT);
    List<Object> bound = new ArrayList<>();
    Matcher named = NAMED.matcher(OutboxClaimSql.CONCURRENT);
    StringBuilder sql = new StringBuilder();
    while (named.find()) {
      String name = named.group(1);
      bound.add(name.equals("until") ? until : values.get(name));
      named.appendReplacement(sql, "?");
    }
    named.appendTail(sql);
    try (Connection c = connect()) {
      c.setAutoCommit(false); // ANALYZE runs the FOR UPDATE: roll the locks back
      try (PreparedStatement st =
          c.prepareStatement("explain (analyze, buffers, format json) " + sql)) {
        for (int i = 0; i < bound.size(); i++) {
          st.setObject(i + 1, bound.get(i));
        }
        try (ResultSet rs = st.executeQuery()) {
          rs.next();
          return new ObjectMapper().readTree(rs.getString(1)).get(0);
        }
      } finally {
        c.rollback();
      }
    }
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
  private static long assertBoundedClaim(JsonNode explained) {
    JsonNode plan = explained.get("Plan");
    List<JsonNode> all = nodes(plan, new ArrayList<>());
    assertThat(all)
        .noneMatch(
            n ->
                n.get("Node Type").asString().equals("Seq Scan")
                    && "outbox".equals(n.path("Relation Name").asString(null)));
    assertThat(all)
        .filteredOn(n -> "outbox_pkey".equals(n.path("Index Name").asString(null)))
        .allSatisfy(n -> assertThat(n.get("Actual Loops").asLong()).isLessThanOrEqualTo(LIMIT));
    long buffers = plan.get("Shared Hit Blocks").asLong() + plan.get("Shared Read Blocks").asLong();
    assertThat(buffers).as("buffers read by one claim").isLessThanOrEqualTo(BUFFER_BOUND);
    return plan.get("Actual Rows").asLong();
  }

  @Test
  @Order(1)
  void idle_aMillionRelayedRows_noneClaimable() throws Exception {
    assertThat(assertBoundedClaim(explainClaim("", null))).isZero();
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

    assertThat(assertBoundedClaim(explainClaim("", null))).isZero(); // the key is busy
  }

  @Test
  @Order(3)
  void theSameBacklog_withItsKeyNotInFlight_claimsExactlyItsHead() throws Exception {
    execute(
        """
        update outbox set claimed_until = null, next_attempt_on = now() - interval '1 second'
        where ordering_key = 'hub:1' and relayed_on is null and claimed_until is not null""");

    assertThat(assertBoundedClaim(explainClaim("", null))).isEqualTo(1);
  }

  @Test
  @Order(4)
  void tenThousandKeysOfTenRows_fromAnyCursor() throws Exception {
    execute(
        "delete from outbox where relayed_on is null",
        """
        insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
          created_on, attempts, next_attempt_on, lane, ordering_key)
        select 't', 'a', 'e', 'hub:k' || (g % 10000), '{}', now(), 0,
          now() - interval '1 second', 'CONCURRENT', 'hub:k' || (g % 10000)
        from generate_series(1, 100000) g""");

    assertThat(assertBoundedClaim(explainClaim("", null))).isEqualTo(LIMIT);
    assertThat(assertBoundedClaim(explainClaim("hub:k5000", null))).isEqualTo(LIMIT);
    // the wrap: keys up to the cursor (string order - "hub:k1000" would bound it to five keys)
    assertThat(assertBoundedClaim(explainClaim("", "hub:k5"))).isEqualTo(LIMIT);
  }
}
