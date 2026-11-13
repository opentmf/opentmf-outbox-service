package org.opentmf.outbox.internal;

import org.opentmf.outbox.OutboxClientProfileResolver;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.OutboxPublisher;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.ObjectMapper;

/**
 * Test-side only: the library's HTTP publisher with its plain client on the JDK's HttpClient, the
 * client a consumer without Apache HttpClient gets - for the invariants pods' second HTTP run
 * ({@code it.http.client=jdk}). The library itself picks Apache on this classpath; this is no
 * production switch.
 */
public final class JdkPlainHttpPublisher {

  private JdkPlainHttpPublisher() {}

  /** The publisher, its plain client's timeouts the library's own. */
  public static OutboxPublisher create(
      OutboxProperties properties,
      OutboxClientProfileResolver profileResolver,
      ObjectMapper objectMapper,
      String serviceName) {
    RestClient plain =
        RestClient.builder()
            .requestFactory(
                OutboxHttpClients.jdk(
                    properties.httpConnectTimeout(), properties.httpReadTimeout()))
            .build();
    return new HttpOutboxPublisher(plain, profileResolver, objectMapper, serviceName);
  }
}
