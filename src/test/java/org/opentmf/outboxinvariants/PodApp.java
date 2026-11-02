package org.opentmf.outboxinvariants;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.opentmf.outbox.OutboxClientProfileResolver;
import org.opentmf.outbox.OutboxRelayedListener;
import org.opentmf.outbox.internal.OutboxClock;
import org.opentmf.outbox.internal.OutboxEventRepository;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.client.RestClient;

/**
 * One pod of a scenario: a consumer-shaped application with the library auto-configured, the
 * scripted publisher for {@code inv:} rows (ahead of the library's defaults) and a relayed listener
 * that throws where the script says so. Its own package: no other IT's context scans it.
 */
@SpringBootApplication
public class PodApp {

  @Bean
  @Order(Ordered.HIGHEST_PRECEDENCE)
  ScriptedPublisher scriptedPublisher(Environment environment) {
    return new ScriptedPublisher(environment.getProperty("it.pod", "pod"));
  }

  /** The post-relay seam, made to throw for a script that says so (C5). */
  @Bean
  OutboxRelayedListener scriptedListener() {
    return event -> {
      if (!event.getDestination().startsWith(ScriptedPublisher.PREFIX)) {
        return;
      }
      Receiver.listened(event.getId());
      if (Receiver.scriptFor(event.getDestination()).listenerThrows) {
        throw new IllegalStateException("scripted relayed-listener failure");
      }
    };
  }

  /**
   * Named HTTP client profiles (F): "good" adds the client header X-Client and X-Tenant=client;
   * "auth" carries a token and, on a 401, refreshes it and retries once (the http-clients token
   * path); any other name resolves to null - the publisher's documented fallback.
   */
  @Bean
  @ConditionalOnProperty(name = "it.http.profiles", havingValue = "true")
  OutboxClientProfileResolver profiles() {
    RestClient good =
        RestClient.builder()
            .defaultHeader("X-Client", "good")
            .defaultHeader("X-Tenant", "client")
            .build();
    AtomicInteger token = new AtomicInteger(1);
    RestClient auth =
        RestClient.builder()
            .requestInterceptor(
                (request, body, execution) -> {
                  request.getHeaders().setBearerAuth("token-" + token.get());
                  ClientHttpResponse response = execution.execute(request, body);
                  if (response.getStatusCode().value() == 401) {
                    response.close();
                    token.incrementAndGet(); // re-authenticate, then once more
                    request.getHeaders().setBearerAuth("token-" + token.get());
                    return execution.execute(request, body);
                  }
                  return response;
                })
            .build();
    return (profile, destination) -> {
      if ("good".equals(profile)) {
        return good;
      }
      if ("auth".equals(profile)) {
        return auth;
      }
      return null;
    };
  }

  /**
   * A pod whose own clock is off by {@code it.clock-skew} (B5): the library's clock runs on it,
   * corrected against the database as the library corrects any pod's - or, with {@code
   * it.clock-corrected=false}, not corrected at all: every instant the library writes or compares
   * then comes from the skewed clock, which is how every pod kept time up to 1.8.0.
   */
  @Bean
  @ConditionalOnProperty(name = "it.clock-skew")
  OutboxClock skewedClock(Environment environment, OutboxEventRepository repository) {
    Duration skew = DurationStyle.detectAndParse(environment.getRequiredProperty("it.clock-skew"));
    boolean corrected = environment.getProperty("it.clock-corrected", Boolean.class, true);
    return new OutboxClock(
        Clock.offset(Clock.systemUTC(), skew), corrected ? repository::databaseNow : null);
  }

  /** The claim probe of the D3 investigation, on {@code it.claim-probe=<dir>} only. */
  @Bean
  @ConditionalOnProperty(name = "it.claim-probe")
  static ClaimProbe claimProbe(Environment environment) {
    return new ClaimProbe(
        Path.of(environment.getRequiredProperty("it.claim-probe")),
        environment.getProperty("spring.application.name", "inv") + "-"
            + environment.getProperty("it.pod", "pod"));
  }

  @Bean
  TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {
    return new TransactionTemplate(transactionManager);
  }
}
