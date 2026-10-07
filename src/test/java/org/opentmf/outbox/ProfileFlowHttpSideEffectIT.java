package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.RouterFunctions;
import org.springframework.web.servlet.function.ServerResponse;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * CONSUMER CONFORMANCE - the dnms-flow profile (H.6, re-stated for 1.3.0): a consumer publisher
 * for the {@code adapter:} scheme, ordered ahead of the defaults, sends in NO transaction and
 * WRITES its consumer row (flow's {@code markRecorded}, carrying the id the send returned) in
 * its BOOKING HOOK - it commits together with {@code relayed_on}; HTTP side effects go through
 * the library's HTTP publisher, now on the CONCURRENT lane. Both are delivered exactly once; the
 * 1.2.x "PI before bounce" id order between them is GONE by design - a CONCURRENT row keeps no
 * order against ORDERED rows (flow's own ordering does not rest on the relay).
 */
@Testcontainers
@SpringBootTest(
    webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = {
      "spring.application.name=dnms-flow",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///flow",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog-flow.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=1s"
    })
class ProfileFlowHttpSideEffectIT {

  /** Every delivery in arrival order: "adapter:<id>" or "http:<id>". */
  static final List<String> DELIVERIES = new CopyOnWriteArrayList<>();

  @TestConfiguration
  static class ConsumerFlow {

    @Bean
    TransactionTemplate transactionTemplate(PlatformTransactionManager txManager) {
      return new TransactionTemplate(txManager);
    }

    @Bean
    JdbcTemplate jdbcTemplate(DataSource dataSource) {
      return new JdbcTemplate(dataSource);
    }

    @Bean
    RouterFunction<ServerResponse> flowReceiver() {
      return RouterFunctions.route()
          .POST(
              "/flow/cb",
              request -> {
                DELIVERIES.add(
                    "http:" + request.headers().firstHeader(OutboxHeaders.IDEMPOTENCY_KEY));
                return ServerResponse.ok().build();
              })
          .build();
    }

    /** flow's adapter publisher: delivers in-process, marks the record in the BOOKING hook. */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    OutboxPublisher adapterPublisher(JdbcTemplate jdbc) {
      return new OutboxPublisher() {
        @Override
        public boolean supports(OutboxEvent event) {
          return event.getDestination().startsWith("adapter:");
        }

        @Override
        public void publish(OutboxEvent event) {
          deliver(event);
        }

        @Override
        public Object deliver(OutboxEvent event) {
          assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
          DELIVERIES.add("adapter:" + OutboxHeaders.idempotencyKey("dnms-flow", event.getId()));
          return OffsetDateTime.now(); // what the receiver answered - the booking records it
        }

        @Override
        public void onBooked(OutboxEvent event, OutboxBooking booking) {
          assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isTrue();
          if (booking.outcome() == OutboxBooking.Outcome.RELAYED) {
            jdbc.update(
                "insert into side_effect (outbox_id, recorded_on) values (?, ?)",
                event.getId(),
                booking.result());
          }
        }
      };
    }
  }

  @Autowired private OutboxWriter writer;
  @Autowired private OutboxMaintenanceService maintenance;
  @Autowired private TransactionTemplate tx;
  @Autowired private JdbcTemplate jdbc;
  @LocalServerPort private int port;

  @Test
  void adapterPublisherWritesInItsBookingHook_andBothRowsDeliverExactlyOnce() {
    String callback = "http://localhost:" + port + "/flow/cb";
    // ONE business transaction appends the PI (adapter) and then the bounce (HTTP)
    List<OutboxEvent> appended =
        tx.execute(
            s ->
                List.of(
                    writer.append("dispatch", "d-1", "pi.recorded.v1", "adapter:pi", Map.of()),
                    writer.append("dispatch", "d-1", "bounce.v1", callback, Map.of())));
    OutboxEvent pi = appended.get(0);
    OutboxEvent bounce = appended.get(1);

    await()
        .atMost(Duration.ofSeconds(20))
        .untilAsserted(
            () -> {
              assertThat(maintenance.inspect(bounce.getId()).relayedOn()).isNotNull();
              assertThat(maintenance.inspect(pi.getId()).relayedOn()).isNotNull();
            });

    // the side effect committed with relayed_on - one booking transaction, both facts or neither
    assertThat(
            jdbc.queryForObject(
                "select count(*) from side_effect where outbox_id = ?", Long.class, pi.getId()))
        .isEqualTo(1L);

    // exactly once each - in either order: the bounce rides the CONCURRENT lane
    assertThat(DELIVERIES)
        .containsExactlyInAnyOrder(
            "adapter:dnms-flow:outbox:" + pi.getId(),
            "http:dnms-flow:outbox:" + bounce.getId());
  }
}
