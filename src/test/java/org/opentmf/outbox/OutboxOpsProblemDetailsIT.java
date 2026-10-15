package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * OUTBOX-OAS-FRAGMENT-SECURITY-RESPONSES-1 (1.7.0) on the real application, the real toolkit and
 * Boot's own RFC 9457 rendering ({@code spring.mvc.problemdetails.enabled=true}): every list-route
 * answer a black-box suite provokes is documented by the shipped fragment on that operation, in the
 * media type it is written in - the toolkit's own 400 and 416 included, which keep their {@code
 * Tmf630Error} beside Boot's problem handler. The row routes ride along, so the standalone {@code
 * OutboxOpsWireConformanceTests} is not the only evidence for them.
 */
@AutoConfigureMockMvc
@SpringBootTest(
    properties = {
      "spring.application.name=outbox-problem-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///problems",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "spring.mvc.problemdetails.enabled=true"
    })
class OutboxOpsProblemDetailsIT {

  @TestConfiguration
  static class TxTemplateConfig {
    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
      return new TransactionTemplate(txManager);
    }
  }

  @Autowired private MockMvc mockMvc;
  @Autowired private OutboxWriter writer;
  @Autowired private TransactionTemplate tx;

  private MediaType answers(String operation, RequestBuilder request, int status) throws Exception {
    return OpsFragmentConformance.answers(mockMvc, operation, request, status);
  }

  @Test
  void theListRoutes_answerAsTheFragmentDocuments() throws Exception {
    String eventType = "it.problems." + UUID.randomUUID();
    tx.execute(status -> writer.append("it-aggregate", "a-1", eventType, "it-problems", Map.of()));

    for (String list :
        new String[] {"/ops/outbox", "/ops/outbox/state/pending", "/ops/outbox/parked"}) {
      String operation = "GET " + (list.contains("/state/") ? "/ops/outbox/state/{state}" : list);
      // the toolkit's own refusals keep its Tmf630Error beside Boot's problem handler
      assertThat(answers(operation, get(list).param("limit", "0"), 400))
          .isEqualTo(MediaType.APPLICATION_JSON);
      assertThat(answers(operation, get(list).param("noSuchField", "x"), 400))
          .isEqualTo(MediaType.APPLICATION_JSON);
      answers(operation, get(list).param("limit", "501"), 200); // clamped, not refused
      assertThat(answers(operation, get(list).accept(MediaType.APPLICATION_XML), 406))
          .isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
    }
    // an unknown state leg is the library's ErrorResponse: the consumer's problem
    assertThat(answers("GET /ops/outbox", get("/ops/outbox").param("state", "dead-lettered"), 400))
        .isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
    assertThat(
            answers("GET /ops/outbox/state/{state}", get("/ops/outbox/state/dead-lettered"), 400))
        .isEqualTo(MediaType.APPLICATION_PROBLEM_JSON);
    assertThat(
            answers(
                "GET /ops/outbox",
                get("/ops/outbox").param("eventType", eventType).param("offset", "99"),
                416))
        .isEqualTo(MediaType.APPLICATION_JSON);
  }

  @Test
  void theRowRoutes_answerAsTheFragmentDocuments() throws Exception {
    answers("GET /ops/outbox/{id}", get("/ops/outbox/987654321"), 404);
    answers("GET /ops/outbox/{id}", get("/ops/outbox/not-a-row"), 404);
    answers("GET /ops/outbox/{id}", get("/ops/outbox/99999999999999999999"), 400);
    answers("POST /ops/outbox/{id}/cancel", post("/ops/outbox/987654321/cancel"), 404);
    answers("POST /ops/outbox/unpark", get("/ops/outbox/unpark"), 405);
    answers(
        "POST /ops/outbox/unpark",
        post("/ops/outbox/unpark").contentType(MediaType.TEXT_PLAIN).content("hub:a"),
        415);
    answers(
        "POST /ops/outbox/unpark",
        post("/ops/outbox/unpark").contentType(MediaType.APPLICATION_JSON).content("{}"),
        400);
    answers(
        "POST /ops/outbox/maintenance/prune",
        post("/ops/outbox/maintenance/prune").accept(MediaType.TEXT_PLAIN),
        406);
  }
}
