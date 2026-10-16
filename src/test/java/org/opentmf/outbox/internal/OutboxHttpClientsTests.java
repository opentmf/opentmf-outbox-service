package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.JdkClientHttpRequestFactory;

/** The plain client's factory: the one RestClient.create() would pick, its timeouts set. */
class OutboxHttpClientsTests {

  /** No Apache, Jetty or Reactor HTTP client on this classpath: the JDK's, as RestClient picks. */
  @Test
  void onThisClasspath_theJdkClient_isPicked() {
    assertThat(OutboxHttpClients.requestFactory(Duration.ofSeconds(1), Duration.ofSeconds(2)))
        .isInstanceOf(JdkClientHttpRequestFactory.class);
    assertThat(OutboxHttpClients.plain(Duration.ofSeconds(1), Duration.ofSeconds(2))).isNotNull();
  }
}
