package org.opentmf.outbox.internal;

import java.net.http.HttpClient;
import java.time.Duration;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.util.Timeout;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.http.client.JettyClientHttpRequestFactory;
import org.springframework.http.client.ReactorClientHttpRequestFactory;
import org.springframework.util.ClassUtils;
import org.springframework.web.client.RestClient;

/**
 * The HTTP publisher's PLAIN client (1.9.0): the very HTTP client {@code RestClient.create()} picks
 * from the classpath - Apache HttpClient 5, Jetty, Reactor Netty, else the JDK's - now with a
 * connect and a read timeout. Up to 1.8.0 it was {@code RestClient.create()} itself, with no
 * timeout at all.
 *
 * <p>Apache HttpClient (1.11.1): Spring 7's factory takes only the read timeout - Apache's per-read
 * response timeout - so up to 1.11.0 the connect timeout stayed Apache's own three minutes, past
 * the default send bound, and a blocking connect on a platform thread ignores the send heartbeat's
 * interrupt: the lease lapsed while the connect went on, another relay sent the row, and the first
 * could still POST it - twice, and two sends to one receiver in flight. The library now builds that
 * client itself, as {@code HttpClients.createSystem()} does, with the connect timeout set on its
 * connections. What remains is Apache's: its response timeout counts per read, so a receiver that
 * drips its response can outlast the bound on a platform thread.
 *
 * <p>Three more of Apache's defaults are off on that client (1.11.1), each a 1.11.0 defect for a
 * consumer with Apache on its classpath: it re-sent a 503 or a 429 once inside the call, so one
 * attempt POSTed twice and the relay's budget never counted the second; it FOLLOWED a 3xx, into a
 * second request booked RELAYED although the receiver never processed the first; and its pool let
 * at most five sends per receiver host be in flight, the rest waiting for a connection inside their
 * send, slot and lease held. Now redirects are not followed, it never retries - the relay is the
 * one that does - and its pool holds as many connections, per route and in all, as the lane sends
 * at once ({@code concurrent.max-in-flight}).
 */
final class OutboxHttpClients {

  private static final ClassLoader LOADER = OutboxHttpClients.class.getClassLoader();

  private OutboxHttpClients() {}

  /** The plain client, with these timeouts, for a lane sending at most {@code maxInFlight}. */
  static RestClient plain(Duration connectTimeout, Duration readTimeout, int maxInFlight) {
    return RestClient.builder()
        .requestFactory(requestFactory(connectTimeout, readTimeout, maxInFlight))
        .build();
  }

  /** The request factory {@code RestClient.create()} would pick, its timeouts set. */
  static ClientHttpRequestFactory requestFactory(
      Duration connectTimeout, Duration readTimeout, int maxInFlight) {
    if (present("org.apache.hc.client5.http.classic.HttpClient")) {
      return Apache.requestFactory(connectTimeout, readTimeout, maxInFlight);
    }
    if (present("org.eclipse.jetty.client.HttpClient")) {
      JettyClientHttpRequestFactory factory = new JettyClientHttpRequestFactory();
      factory.setConnectTimeout(connectTimeout);
      factory.setReadTimeout(readTimeout);
      return factory;
    }
    if (present("reactor.netty.http.client.HttpClient")) {
      ReactorClientHttpRequestFactory factory = new ReactorClientHttpRequestFactory();
      factory.setConnectTimeout(connectTimeout);
      factory.setReadTimeout(readTimeout);
      return factory;
    }
    return jdk(connectTimeout, readTimeout);
  }

  /**
   * The JDK's client. Spring times its read timeout from the request's start, so it bounds the
   * connect and the response together.
   */
  static ClientHttpRequestFactory jdk(Duration connectTimeout, Duration readTimeout) {
    JdkClientHttpRequestFactory factory =
        new JdkClientHttpRequestFactory(
            HttpClient.newBuilder().connectTimeout(connectTimeout).build());
    factory.setReadTimeout(readTimeout);
    return factory;
  }

  /**
   * Apache HttpClient 5, linked only when it is on the classpath: a nested class, so the outer one
   * never loads its types on a classpath without it.
   */
  static final class Apache {

    private Apache() {}

    /**
     * Apache's system client: its connections' connect timeout and its response timeout set, no
     * redirect followed, no retry of its own, and a pool of {@code maxInFlight} connections.
     */
    static ClientHttpRequestFactory requestFactory(
        Duration connectTimeout, Duration readTimeout, int maxInFlight) {
      PoolingHttpClientConnectionManager connections =
          PoolingHttpClientConnectionManagerBuilder.create()
              .useSystemProperties()
              .setDefaultConnectionConfig(
                  ConnectionConfig.custom().setConnectTimeout(Timeout.of(connectTimeout)).build())
              .setMaxConnPerRoute(maxInFlight)
              .setMaxConnTotal(maxInFlight)
              .build();
      HttpComponentsClientHttpRequestFactory factory =
          new HttpComponentsClientHttpRequestFactory(
              HttpClients.custom()
                  .useSystemProperties()
                  .setConnectionManager(connections)
                  .disableRedirectHandling()
                  .disableAutomaticRetries()
                  .build());
      factory.setReadTimeout(readTimeout);
      return factory;
    }
  }

  private static boolean present(String className) {
    return ClassUtils.isPresent(className, LOADER);
  }
}
