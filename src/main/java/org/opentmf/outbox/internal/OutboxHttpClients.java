package org.opentmf.outbox.internal;

import java.net.http.HttpClient;
import java.time.Duration;
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
 * timeout at all. Apache HttpClient takes the read timeout only (its connect timeout is its own,
 * three minutes); the send heartbeat bounds a slow connect there.
 */
final class OutboxHttpClients {

  private static final ClassLoader LOADER = OutboxHttpClients.class.getClassLoader();

  private OutboxHttpClients() {}

  /** The plain client, with these timeouts. */
  static RestClient plain(Duration connectTimeout, Duration readTimeout) {
    return RestClient.builder().requestFactory(requestFactory(connectTimeout, readTimeout)).build();
  }

  /** The request factory {@code RestClient.create()} would pick, its timeouts set. */
  static ClientHttpRequestFactory requestFactory(Duration connectTimeout, Duration readTimeout) {
    if (present("org.apache.hc.client5.http.classic.HttpClient")) {
      HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
      factory.setReadTimeout(readTimeout);
      return factory;
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
    JdkClientHttpRequestFactory factory =
        new JdkClientHttpRequestFactory(
            HttpClient.newBuilder().connectTimeout(connectTimeout).build());
    factory.setReadTimeout(readTimeout);
    return factory;
  }

  private static boolean present(String className) {
    return ClassUtils.isPresent(className, LOADER);
  }
}
