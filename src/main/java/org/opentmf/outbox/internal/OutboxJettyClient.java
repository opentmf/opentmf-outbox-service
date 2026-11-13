package org.opentmf.outbox.internal;

import java.time.Duration;
import org.eclipse.jetty.client.HttpClient;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.JettyClientHttpRequestFactory;

/**
 * Jetty's HTTP client for the plain client (1.11.1, OUTBOX-JETTY-FOLLOWS-NO-REDIRECT-1), linked
 * only when Jetty is on the classpath and Apache HttpClient is not: {@link OutboxHttpClients} names
 * this class only on that branch, so a classpath without Jetty never loads its types.
 *
 * <p>Jetty follows a 3xx by default and Spring's factory leaves it so: up to 1.11.0 a consumer with
 * Jetty had a redirect followed into a second request, whose 2xx booked the row RELAYED although
 * the receiver never processed the first. Now no redirect is followed - the 3xx reaches the
 * publisher's own refusal. Its timeouts are as before (the read a total from the request's
 * queueing, the connect included), it never retries, and it holds as many connections per
 * destination as the lane sends at once.
 */
final class OutboxJettyClient {

  private OutboxJettyClient() {}

  /** Jetty's client: no redirect followed, its timeouts set, {@code maxInFlight} connections. */
  static ClientHttpRequestFactory requestFactory(
      Duration connectTimeout, Duration readTimeout, int maxInFlight) {
    HttpClient client = new HttpClient();
    client.setFollowRedirects(false);
    client.setMaxConnectionsPerDestination(maxInFlight);
    JettyClientHttpRequestFactory factory = new JettyClientHttpRequestFactory(client);
    factory.setConnectTimeout(connectTimeout);
    factory.setReadTimeout(readTimeout);
    return factory;
  }
}
