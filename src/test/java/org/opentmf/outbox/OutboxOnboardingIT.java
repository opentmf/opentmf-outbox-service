package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import liquibase.Scope;
import liquibase.command.CommandScope;
import liquibase.resource.DirectoryResourceAccessor;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The library changelog against every table shape a consumer can bring (H.5): a fresh schema
 * installs; a pre-library 1.0.0-shaped table and a 681-shaped one (NOT NULL release_at, extra
 * columns) are ONBOARDED - 001/002 MARK_RAN, 003 adds the missing columns; and a database
 * migrated by the RELEASED 1.1.0 changelog upgrades with its recorded checksums intact (the
 * 1.2.0 preconditions live outside the SQL bodies). Plain Liquibase + JDBC: the scenario is
 * the changelog itself, not the Spring wiring.
 */
@Testcontainers
class OutboxOnboardingIT {

  @Container
  static final PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:18.1-alpine3.22");

  private static final String LIBRARY_CHANGELOG = "db/changelog/opentmf-outbox.sql";
  private static final String RELEASED_1_1_0_CHANGELOG = "db/changelog-1.1.0/opentmf-outbox.sql";
  private static final List<String> LIBRARY_COLUMNS =
      List.of(
          "client_profile", "release_at", "cancelled_on", "parked_on", "reference", "relayed_on");

  private static String urlFor(String database) {
    return postgres.getJdbcUrl().replaceAll("/[^/?]+(\\?|$)", "/" + database + "$1");
  }

  private static String freshDatabase(String name) throws SQLException {
    try (Connection admin =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement st = admin.createStatement()) {
      st.execute("create database " + name);
    }
    return urlFor(name);
  }

  private static void liquibaseUpdate(String url, String changelog) throws Exception {
    liquibaseUpdate(url, changelog, null);
  }

  /** {@code searchPath} null = the classpath; else a directory whose files shadow it. */
  private static void liquibaseUpdate(String url, String changelog, Path searchPath)
      throws Exception {
    CommandScope update =
        new CommandScope("update")
            .addArgumentValue("url", url)
            .addArgumentValue("username", postgres.getUsername())
            .addArgumentValue("password", postgres.getPassword())
            .addArgumentValue("changelogFile", changelog);
    if (searchPath == null) {
      update.execute();
      return;
    }
    // the ONLY resource root: the classpath (and its 1.2.0 file) is out of reach here
    Scope.child(
        Scope.Attr.resourceAccessor,
        new DirectoryResourceAccessor(searchPath),
        update::execute);
  }

  /**
   * The RELEASED 1.1.0 changelog under the SAME path a consumer includes it by - so the rows it
   * records carry the identity ({@code db/changelog/opentmf-outbox.sql}) the 1.2.0 file must
   * be recognised against.
   */
  private static Path released110ChangelogRoot() throws Exception {
    Path root = Files.createTempDirectory("outbox-1.1.0");
    Path file = root.resolve(LIBRARY_CHANGELOG);
    Files.createDirectories(file.getParent());
    try (var in =
        OutboxOnboardingIT.class.getClassLoader().getResourceAsStream(RELEASED_1_1_0_CHANGELOG)) {
      Files.copy(in, file);
    }
    return root;
  }

  private static void execute(String url, String... sql) throws SQLException {
    try (Connection c =
            DriverManager.getConnection(url, postgres.getUsername(), postgres.getPassword());
        Statement st = c.createStatement()) {
      for (String s : sql) {
        st.execute(s);
      }
    }
  }

  /** {@code id -> exectype} from DATABASECHANGELOG, in execution order. */
  private static Map<String, String> changelogRows(String url) throws SQLException {
    Map<String, String> rows = new LinkedHashMap<>();
    try (Connection c =
            DriverManager.getConnection(url, postgres.getUsername(), postgres.getPassword());
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "select id, exectype from databasechangelog where author = 'opentmf-outbox'"
                    + " order by orderexecuted")) {
      while (rs.next()) {
        rows.put(rs.getString(1), rs.getString(2));
      }
    }
    return rows;
  }

  /** Every library DATABASECHANGELOG row as {@code id|filename|md5sum|exectype}. */
  private static List<String> rawRows(String url) throws SQLException {
    List<String> rows = new java.util.ArrayList<>();
    try (Connection c =
            DriverManager.getConnection(url, postgres.getUsername(), postgres.getPassword());
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "select id, filename, md5sum, exectype from databasechangelog where author ="
                    + " 'opentmf-outbox' order by orderexecuted")) {
      while (rs.next()) {
        rows.add(
            rs.getString(1) + "|" + rs.getString(2) + "|" + rs.getString(3) + "|"
                + rs.getString(4));
      }
    }
    return rows;
  }

  private static Map<String, Boolean> columnNullability(String url) throws SQLException {
    Map<String, Boolean> nullable = new LinkedHashMap<>();
    try (Connection c =
            DriverManager.getConnection(url, postgres.getUsername(), postgres.getPassword());
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "select column_name, is_nullable from information_schema.columns where"
                    + " table_name = 'outbox'")) {
      while (rs.next()) {
        nullable.put(rs.getString(1), "YES".equals(rs.getString(2)));
      }
    }
    return nullable;
  }

  private static String pendingIndexPredicate(String url) throws SQLException {
    try (Connection c =
            DriverManager.getConnection(url, postgres.getUsername(), postgres.getPassword());
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "select indexdef from pg_indexes where indexname = 'ix_outbox_pending'")) {
      return rs.next() ? rs.getString(1) : null;
    }
  }

  @Test
  void aFreshSchema_installsAllThreeChangesets() throws Exception {
    String url = freshDatabase("fresh");

    liquibaseUpdate(url, LIBRARY_CHANGELOG);

    assertThat(changelogRows(url))
        .containsExactly(
            Map.entry("001-outbox", "EXECUTED"),
            Map.entry("002-outbox-hold-and-cancel", "EXECUTED"),
            Map.entry("003-outbox-policy-reference-onboarding", "EXECUTED"));
    assertThat(columnNullability(url)).containsKeys(LIBRARY_COLUMNS.toArray(String[]::new));
    assertThat(pendingIndexPredicate(url))
        .contains("relayed_on IS NULL")
        .contains("cancelled_on IS NULL")
        .contains("parked_on IS NULL");
  }

  @Test
  void aPreLibrary100ShapedTable_isOnboarded_001MarkedRan_theRestAddsColumns() throws Exception {
    String url = freshDatabase("shape100");
    // the 1.0.0 shape as a hand-written module left it - no client_profile even
    execute(
        url,
        """
        create table outbox (
          id bigint generated always as identity primary key,
          aggregate_type varchar(64) not null, aggregate_id varchar(128) not null,
          event_type varchar(100) not null, destination varchar(200) not null,
          payload text not null, headers text,
          created_on timestamp with time zone not null,
          attempts smallint not null default 0,
          next_attempt_on timestamp with time zone not null,
          relayed_on timestamp with time zone, last_error text)""",
        "create index ix_outbox_pending on outbox (next_attempt_on) where relayed_on is null",
        "insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,"
            + " created_on, next_attempt_on) values ('t', 'a', 'e', 'topic', '{}', now(), now())");

    liquibaseUpdate(url, LIBRARY_CHANGELOG);

    assertThat(changelogRows(url))
        .containsExactly(
            Map.entry("001-outbox", "MARK_RAN"),
            Map.entry("002-outbox-hold-and-cancel", "EXECUTED"),
            Map.entry("003-outbox-policy-reference-onboarding", "EXECUTED"));
    assertThat(columnNullability(url)).containsKeys(LIBRARY_COLUMNS.toArray(String[]::new));
    assertThat(pendingIndexPredicate(url)).contains("parked_on IS NULL");
  }

  @Test
  void a681ShapedTable_isOnboarded_001And002MarkedRan_theConsumerDeltaStaysTheConsumers()
      throws Exception {
    String url = freshDatabase("shape681");
    execute(
        url,
        """
        create table outbox (
          id bigint generated always as identity primary key,
          aggregate_type varchar(64) not null, aggregate_id varchar(128) not null,
          event_type varchar(100) not null, destination varchar(200) not null,
          payload text not null, headers text,
          created_on timestamp with time zone not null,
          attempts smallint not null default 0,
          next_attempt_on timestamp with time zone not null,
          release_at timestamp with time zone not null default now(),
          relayed_on timestamp with time zone, cancelled_on timestamp with time zone,
          last_error text, kind varchar(32), subscription_id varchar(64))""");

    liquibaseUpdate(url, LIBRARY_CHANGELOG);

    assertThat(changelogRows(url))
        .containsExactly(
            Map.entry("001-outbox", "MARK_RAN"),
            Map.entry("002-outbox-hold-and-cancel", "MARK_RAN"),
            Map.entry("003-outbox-policy-reference-onboarding", "EXECUTED"));
    Map<String, Boolean> nullability = columnNullability(url);
    assertThat(nullability)
        .containsKeys(LIBRARY_COLUMNS.toArray(String[]::new))
        .containsKeys("kind", "subscription_id"); // the library drops nothing
    // the library does NOT touch the consumer's NOT NULL: dropping it is the consumer's delta
    assertThat(nullability.get("release_at")).isFalse();
    execute(
        url,
        "alter table outbox alter column release_at drop not null",
        "alter table outbox alter column release_at drop default");
    assertThat(columnNullability(url).get("release_at")).isTrue();
  }

  @Test
  void aDatabaseMigratedByTheReleased110Changelog_upgradesWithItsChecksumsIntact()
      throws Exception {
    String url = freshDatabase("from110");
    // exactly what a 1.1.0 consumer recorded: the released file, under the included path
    liquibaseUpdate(url, LIBRARY_CHANGELOG, released110ChangelogRoot());
    List<String> recordedBy110 = rawRows(url);
    assertThat(recordedBy110).hasSize(2);

    // the 1.2.0 changelog: a checksum change on 001/002 would fail validation right here
    liquibaseUpdate(url, LIBRARY_CHANGELOG);

    // the two recorded rows are UNTOUCHED (same filename identity, same checksum, still
    // EXECUTED - not re-run, not re-inserted under another path) and 003 was added
    List<String> after = rawRows(url);
    assertThat(after).hasSize(3).startsWith(recordedBy110.toArray(String[]::new));
    assertThat(after.get(2))
        .startsWith("003-outbox-policy-reference-onboarding|")
        .endsWith("|EXECUTED");
    assertThat(columnNullability(url)).containsKeys("parked_on", "reference");
    // and a second run is a no-op (003 is idempotent and recorded)
    liquibaseUpdate(url, LIBRARY_CHANGELOG);
    assertThat(changelogRows(url)).hasSize(3);
  }
}
