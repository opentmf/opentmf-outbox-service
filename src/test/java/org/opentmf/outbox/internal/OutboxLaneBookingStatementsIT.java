package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxTestApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The round trips of the relay's transactions, read from PostgreSQL's own statement log ({@code
 * log_statement = all}), as the coordinator's ruling of 2026-10-13 pins them: a CONCURRENT row's
 * RELAYED booking is {@code BEGIN}, ONE guarded {@code update ... returning}, {@code COMMIT} -
 * nothing else, in particular no {@code SHOW TRANSACTION ISOLATION LEVEL} (the lane's bookings run
 * at the application's default isolation); the CLAIM keeps its READ COMMITTED pin - its transaction
 * opens with that {@code SHOW} - and tries every candidate key's lock and stamps every lease in one
 * statement each.
 */
@Testcontainers
@Import(OutboxLaneBookingStatementsIT.Publishers.class)
@SpringBootTest(
    classes = OutboxTestApplication.class,
    properties = {
      "spring.application.name=outbox-statements-it",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=1h"
    })
class OutboxLaneBookingStatementsIT {

  @Container
  static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:18.1-alpine3.22")
          .withCommand(
              "postgres",
              "-c",
              "fsync=off",
              "-c",
              "log_statement=all",
              "-c",
              "log_line_prefix=%p ");

  /** {@code <pid> LOG: statement: <sql>} or {@code <pid> LOG: execute <name>: <sql>}. */
  private static final Pattern STATEMENT =
      Pattern.compile("^(\\d+) LOG:  (?:statement|execute [^:]+): (.*)$");

  @Autowired OutboxRelayWorker worker;

  @DynamicPropertySource
  static void datasource(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
  }

  @TestConfiguration
  static class Publishers {

    /** "stmt:" rows: CONCURRENT, keyed by aggregate, delivered at once. */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    OutboxPublisher statementsPublisher() {
      return new OutboxPublisher() {
        @Override
        public boolean supports(OutboxEvent event) {
          return event.getDestination().startsWith("stmt:");
        }

        @Override
        public Lane lane(OutboxEvent event) {
          return Lane.CONCURRENT;
        }

        @Override
        public String orderingKey(OutboxEvent event) {
          return event.getAggregateId();
        }

        @Override
        public void publish(OutboxEvent event) {
          // delivered
        }
      };
    }
  }

  private static Connection direct() throws SQLException {
    return DriverManager.getConnection(
        POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
  }

  private static long pending() throws SQLException {
    try (Connection c = direct();
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "select count(*) from outbox where relayed_on is null"
                    + " and destination = 'stmt:topic'")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  /** Every session's statements, in order, by backend pid. */
  private static Map<String, List<String>> statementsBySession() {
    Map<String, List<String>> sessions = new LinkedHashMap<>();
    for (String line : POSTGRES.getLogs().split("\n")) {
      Matcher statement = STATEMENT.matcher(line.strip());
      if (statement.matches()) {
        sessions
            .computeIfAbsent(statement.group(1), pid -> new ArrayList<>())
            .add(statement.group(2).strip());
      }
    }
    return sessions;
  }

  /**
   * Around EVERY statement starting with {@code head}: the two before it ({@code ""} at a session's
   * start) and the one after it.
   */
  private static List<List<String>> around(Map<String, List<String>> sessions, String head) {
    List<List<String>> found = new ArrayList<>();
    for (List<String> statements : sessions.values()) {
      for (int i = 0; i < statements.size() - 1; i++) {
        if (statements.get(i).startsWith(head)) {
          found.add(
              List.of(
                  i >= 2 ? statements.get(i - 2) : "",
                  i >= 1 ? statements.get(i - 1) : "",
                  statements.get(i),
                  statements.get(i + 1)));
        }
      }
    }
    assertThat(found).as("statements starting with %s", head).isNotEmpty();
    return found;
  }

  @Test
  void aConcurrentBooking_isBeginOneUpdateCommit_whileTheClaimKeepsItsPin() throws Exception {
    try (Connection c = direct();
        Statement st = c.createStatement()) {
      st.execute(
          """
          insert into outbox (aggregate_type, aggregate_id, event_type, destination, payload,
            created_on, attempts, next_attempt_on, lane, ordering_key)
          select 'stmt', 'agg-' || g, 'e', 'stmt:topic', '{}', now(), 0,
            now() - interval '1 second', 'CONCURRENT', 'agg-' || g
          from generate_series(1, 3) g\
          """);
    }

    worker.relayBatch(false);
    await().atMost(Duration.ofSeconds(10)).until(() -> pending() == 0);

    Map<String, List<String>> sessions = statementsBySession();
    assertThat(around(sessions, "with booked as"))
        .hasSize(3)
        .allSatisfy(
            booking -> {
              assertThat(booking.get(0)).doesNotStartWith("SHOW TRANSACTION ISOLATION LEVEL");
              assertThat(booking.get(1)).isEqualTo("BEGIN");
              assertThat(booking.get(3)).isEqualTo("COMMIT");
            });
    // every CONCURRENT claim keeps its pin (the lane's nudges run more passes; a wrap pass is a
    // second claim statement in the same transaction)
    assertThat(around(sessions, "with recursive"))
        .filteredOn(claim -> claim.get(1).equals("BEGIN"))
        .isNotEmpty()
        .allSatisfy(
            claim -> assertThat(claim.get(0)).isEqualTo("SHOW TRANSACTION ISOLATION LEVEL"));
    // the claim's writes: ONE key-lock statement and ONE stamp for the three rows
    List<String> all = sessions.values().stream().flatMap(List::stream).toList();
    assertThat(all).filteredOn(s -> s.startsWith("select k.ordering_key from unnest")).hasSize(1);
    assertThat(all).filteredOn(s -> s.startsWith("update outbox set claimed_until")).hasSize(1);
  }
}
