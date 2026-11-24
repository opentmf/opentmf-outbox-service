package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerResponse;

/**
 * A service's own {@link OutboxFailureText} (1.12.0), against the real engines: the library's HTTP
 * publisher posts to the service's own web server, which refuses with a 400 whose body carries a
 * made-up value. The row parks after its one attempt; its stored and served {@code lastError} is
 * the service's text - the kind kept, the receiver's body not. A row the bean leaves to {@link
 * OutboxFailureText#DEFAULT} stores the text up to 1.11.0, the body included.
 */
@SpringBootTest(
    webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = {
      "spring.application.name=outbox-failure-text-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///failuretext",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=1h",
      "opentmf.outbox.metrics-refresh=1h",
      "opentmf.outbox.max-attempts=1"
    })
@DirtiesContext
class OutboxFailureTextIT {

  /**
   * What the receiver answers - the value no row and no view may carry under the service's text.
   */
  static final String PEER_BODY = "{\"reason\":\"made-up-value-5173\"}";

  @TestConfiguration
  static class Service {

    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
      return new TransactionTemplate(txManager);
    }

    @Bean
    RouterFunction<ServerResponse> refusingReceiver() {
      return RouterFunctions.route()
          .POST(
              "/stub/refuse/{variant}",
              request -> ServerResponse.status(HttpStatus.BAD_REQUEST).body(PEER_BODY))
          .build();
    }

    /** The service's text: the kind and a fixed word; a row to "/default" keeps today's text. */
    @Bean
    OutboxFailureText serviceFailureText() {
      return (row, failure) ->
          row.destination().endsWith("/default")
              ? OutboxFailureText.DEFAULT.describe(row, failure)
              : OutboxFailureText.typed(failure, "the receiver refused (" + row.attempts() + ")");
    }
  }

  @Autowired private OutboxWriter writer;
  @Autowired private OutboxMaintenanceService maintenance;
  @Autowired private TransactionTemplate tx;
  @LocalServerPort private int port;

  private OutboxEvent append(String variant) {
    String url = "http://localhost:" + port + "/stub/refuse/" + variant;
    return tx.execute(s -> writer.append("text", "t-" + variant, "text.v1", url, Map.of()));
  }

  private OutboxRowView parked(OutboxEvent row) {
    await()
        .atMost(Duration.ofSeconds(60))
        .until(() -> maintenance.inspect(row.getId()).parkedOn() != null);
    return maintenance.inspect(row.getId());
  }

  @Test
  void aRefusedRow_storesAndServesTheServicesText_withoutTheReceiversBody() {
    OutboxEvent row = append("own");

    OutboxRowView view = parked(row);

    String expected = "BadRequest: the receiver refused (1)";
    assertThat(view.lastError()).isEqualTo(expected);
    assertThat(maintenance.payload(row.getId()).lastError()).isEqualTo(expected);
    assertThat(OutboxRowView.kindOf(expected)).isEqualTo("BadRequest");
  }

  @Test
  void aRowTheServiceLeavesToTheDefault_storesTheTextUpTo1110_theBodyIncluded() {
    OutboxEvent row = append("default");

    OutboxRowView view = parked(row);

    assertThat(view.lastError()).startsWith("BadRequest: 400").contains(PEER_BODY);
  }
}
