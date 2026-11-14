package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import liquibase.Scope;
import liquibase.command.CommandScope;
import liquibase.resource.DirectoryResourceAccessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxHeaders;
import org.opentmf.outbox.OutboxTestApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The start-time schema check (OUTBOX-IDEMPOTENCY-KEY-REPEATS-AFTER-RESTORE-1, changeset 009), each
 * case over a database the RELEASED 1.11.0 changelog migrated - recorded under the path a consumer
 * includes it by, as a 1.11.0 consumer's database is. A deployment that runs the library's
 * changelog OUT OF BAND and starts this version on that schema is refused at start with one error
 * naming the changeset and its statement - whether Hibernate validates the schema or not. A
 * consumer that includes the changelog by reference upgrades with nothing to do: Liquibase applies
 * 009 at that start, before the check runs.
 */
@Testcontainers
class OutboxSchemaCheckIT {

  @Container
  static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.1-alpine3.22");

  private static final String LIBRARY_CHANGELOG = "db/changelog/opentmf-outbox.sql";
  private static final String RELEASED_1_11_0 = "db/changelog-1.11.0/opentmf-outbox.sql";
  private static final AtomicInteger DATABASES = new AtomicInteger();

  private String url;

  /** A fresh database at the 1.11.0 schema: changesets 001-008, no key_token. */
  @BeforeEach
  void a1110Database() throws Exception {
    String name = "schema" + DATABASES.incrementAndGet();
    sql(postgres.getJdbcUrl(), "create database " + name);
    url = postgres.getJdbcUrl().replaceAll("/[^/?]+(\\?|$)", "/" + name + "$1");
    Path root = Files.createTempDirectory("outbox-1110");
    Path file = root.resolve(LIBRARY_CHANGELOG);
    Files.createDirectories(file.getParent());
    try (var in = getClass().getClassLoader().getResourceAsStream(RELEASED_1_11_0)) {
      Files.copy(in, file);
    }
    CommandScope update =
        new CommandScope("update")
            .addArgumentValue("url", url)
            .addArgumentValue("username", postgres.getUsername())
            .addArgumentValue("password", postgres.getPassword())
            .addArgumentValue("changelogFile", LIBRARY_CHANGELOG);
    Scope.child(Scope.Attr.resourceAccessor, new DirectoryResourceAccessor(root), update::execute);
  }

  private ConfigurableApplicationContext start(boolean liquibase, String ddlAuto) {
    return new SpringApplicationBuilder(OutboxTestApplication.class)
        .web(WebApplicationType.NONE)
        .properties(
            "spring.application.name=outbox-schema-check-it",
            "spring.main.banner-mode=off",
            "spring.datasource.url=" + url,
            "spring.datasource.username=" + postgres.getUsername(),
            "spring.datasource.password=" + postgres.getPassword(),
            "spring.liquibase.change-log=classpath:db/test-changelog.xml",
            "spring.liquibase.enabled=" + liquibase,
            "spring.jpa.hibernate.ddl-auto=" + ddlAuto,
            "opentmf.outbox.sweep-interval=1s")
        .run();
  }

  private static void sql(String url, String... statements) throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(url, postgres.getUsername(), postgres.getPassword());
        Statement statement = connection.createStatement()) {
      for (String each : statements) {
        statement.execute(each);
      }
    }
  }

  /** The start fails, and the library's error - naming 009 and its statement - is the cause. */
  private void assertRefusedNaming009(boolean liquibase, String ddlAuto) {
    Throwable failure = catchThrowable(() -> start(liquibase, ddlAuto).close());
    assertThat(failure).as("the start is refused").isNotNull();
    Throwable cause = failure;
    while (cause != null && !(cause instanceof IllegalStateException)) {
      cause = cause.getCause();
    }
    assertThat(cause)
        .as("the library's error is in the cause chain")
        .isNotNull()
        .hasMessageContaining("The outbox table has no column key_token")
        .hasMessageContaining("opentmf-outbox changeset " + OutboxSchemaCheck.CHANGESET)
        .hasMessageContaining(OutboxSchemaCheck.STATEMENT);
  }

  @Test
  void theChangelogRunOutOfBand_withoutSchemaValidation_refusesTheStart_naming009() {
    assertRefusedNaming009(false, "none");
  }

  /** The check runs before the EntityManagerFactory: its error, not Hibernate's, is the one. */
  @Test
  void theChangelogRunOutOfBand_withSchemaValidation_refusesTheStart_naming009() {
    assertRefusedNaming009(false, "validate");
  }

  /**
   * The changelog included by reference - a 1.11.0 consumer that changes nothing but the version:
   * Liquibase applies 009 at this start, then the check passes, and a row booked under 1.11.0 (no
   * token) is relayed under exactly the key 1.11.0 gave it.
   */
  @Test
  void theChangelogIncludedByReference_upgradesWithNothingToDo_andRelaysAn1110Row()
      throws Exception {
    List<String> received = new CopyOnWriteArrayList<>();
    HttpServer receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    receiver.createContext(
        "/consumer",
        exchange -> {
          received.add(exchange.getRequestHeaders().getFirst(OutboxHeaders.IDEMPOTENCY_KEY));
          exchange.getRequestBody().readAllBytes();
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });
    receiver.start();
    try {
      sql(
          url,
          "insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,"
              + " created_on, attempts, next_attempt_on) values ('a', 'b', 'e', 'http://127.0.0.1:"
              + receiver.getAddress().getPort()
              + "/consumer', '{}', now(), 0, now())");
      try (ConfigurableApplicationContext context = start(true, "validate")) {
        assertThat(context.isRunning()).isTrue();
        await()
            .atMost(Duration.ofSeconds(30))
            .untilAsserted(
                () -> assertThat(received).containsExactly("outbox-schema-check-it:outbox:1"));
      }
      sql(url, "select key_token from outbox limit 0");
    } finally {
      receiver.stop(0);
    }
  }

  /** The statement the error names is the one a deployment applies; then the start goes on. */
  @Test
  void theNamedStatementApplied_theStartGoesOn() throws SQLException {
    sql(url, OutboxSchemaCheck.STATEMENT);
    try (ConfigurableApplicationContext context = start(false, "validate")) {
      assertThat(context.isRunning()).isTrue();
    }
  }
}
