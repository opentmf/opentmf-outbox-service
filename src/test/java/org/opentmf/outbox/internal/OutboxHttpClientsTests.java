package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opentmf.outbox.internal.KafkaLessStartupTests.ConsumerClassLoader;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.http.client.JettyClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

/**
 * The plain client's factory: the one RestClient.create() would pick, its timeouts set - the
 * CONNECT timeout too on Apache HttpClient (1.11.1,
 * OUTBOX-HTTP-CONNECT-TIMEOUT-WITHIN-SEND-BOUND-1), whose own is three minutes.
 */
class OutboxHttpClientsTests {

  private static final Duration CONNECT = Duration.ofSeconds(1);
  private static final Duration READ = Duration.ofSeconds(5);
  private static final int SLOTS = 8; // concurrent.max-in-flight's default

  /** The library compiles against Apache HttpClient, so it is on this classpath: it is picked. */
  @Test
  void whenApacheIsPresent_ApacheIsPicked() {
    assertThat(OutboxHttpClients.requestFactory(CONNECT, READ, SLOTS))
        .isInstanceOf(HttpComponentsClientHttpRequestFactory.class);
    assertThat(OutboxHttpClients.plain(CONNECT, READ, SLOTS)).isNotNull();
  }

  /** A consumer with Jetty and no Apache: Jetty's, as RestClient picks, built by the library. */
  @Test
  void withJettyAndNoApache_JettyIsPicked() throws Exception {
    try (ConsumerClassLoader withoutApache = new ConsumerClassLoader("org.apache.hc.")) {
      assertThat(requestFactoryIn(withoutApache)).isInstanceOf(JettyClientHttpRequestFactory.class);
    }
  }

  /**
   * A consumer with neither Apache HttpClient nor Jetty: the JDK's client, as RestClient picks -
   * and nothing of either is linked on the way (their classes are named on their branches only).
   */
  @Test
  void withoutApacheAndJetty_theJdkClientIsPicked() throws Exception {
    try (ConsumerClassLoader neither =
        new ConsumerClassLoader("org.apache.hc.", "org.eclipse.jetty.")) {
      assertThat(requestFactoryIn(neither)).isInstanceOf(JdkClientHttpRequestFactory.class);
    }
  }

  /** The plain client's factory as a consumer with that classpath gets it. */
  private static Object requestFactoryIn(ConsumerClassLoader consumer) throws Exception {
    Method requestFactory =
        consumer
            .loadClass(OutboxHttpClients.class.getName())
            .getDeclaredMethod("requestFactory", Duration.class, Duration.class, int.class);
    requestFactory.setAccessible(true); // package-private, in the consumer's own loader
    return requestFactory.invoke(null, CONNECT, READ, SLOTS);
  }

  // ------------------------------------------------------------ a connect that stalls

  private final List<Socket> backlog = new ArrayList<>();
  private ServerSocket listener;

  @AfterEach
  void closeTheListener() throws IOException {
    for (Socket socket : backlog) {
      socket.close();
    }
    if (listener != null) {
      listener.close();
    }
  }

  /**
   * HOST PROBE: a local listener that never accepts, its accept queue filled until a connect stalls
   * - Linux drops the SYN of a connect to a full queue, so the next connect hangs until its
   * timeout. A host that refuses or completes it instead cannot stall a connect this way: the cell
   * ends as that named assumption, never silently green.
   */
  private InetSocketAddress aStallingAddress() throws IOException {
    listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
    InetSocketAddress address =
        new InetSocketAddress(InetAddress.getLoopbackAddress(), listener.getLocalPort());
    for (int i = 0; i < 64; i++) {
      Socket socket = new Socket();
      try {
        socket.connect(address, 300);
        backlog.add(socket);
      } catch (SocketTimeoutException stalled) {
        socket.close();
        return address; // the queue is full: the next connect stalls too
      } catch (IOException refused) {
        socket.close();
        break;
      }
    }
    Assumptions.abort(
        "HOST PROBE: this host ("
            + System.getProperty("os.name")
            + ") does not stall a connect to a full accept queue - the stalled-connect cell needs"
            + " Linux's behaviour; not run here");
    return address;
  }

  /** Posts through the factory and returns how long the call took before it failed. */
  private static Duration postTimed(ClientHttpRequestFactory factory, InetSocketAddress address) {
    RestClient client = RestClient.builder().requestFactory(factory).build();
    long started = System.nanoTime();
    Throwable failure =
        catchThrowable(
            () ->
                client
                    .post()
                    .uri("http://127.0.0.1:" + address.getPort() + "/hook")
                    .body("{}")
                    .retrieve()
                    .toBodilessEntity());
    assertThat(failure).as("a stalled connect fails").isNotNull();
    return Duration.ofNanos(System.nanoTime() - started);
  }

  /** Up to 1.11.0 Apache's connect ran its own three minutes, past the default send bound. */
  @Test
  void apache_endsAStalledConnectAtTheConnectTimeout() throws IOException {
    InetSocketAddress address = aStallingAddress();
    ClientHttpRequestFactory apache = OutboxHttpClients.Apache.requestFactory(CONNECT, READ, SLOTS);

    Duration took =
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> postTimed(apache, address));

    assertThat(took).isLessThan(CONNECT.plusSeconds(2));
  }

  /** The JDK's client bounds a stalled connect too (a control: it did before 1.11.1 as well). */
  @Test
  void jdk_endsAStalledConnectAtTheConnectTimeout() throws IOException {
    InetSocketAddress address = aStallingAddress();
    ClientHttpRequestFactory jdk = OutboxHttpClients.jdk(CONNECT, READ);

    Duration took =
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> postTimed(jdk, address));

    assertThat(took).isLessThan(CONNECT.plusSeconds(2));
  }

  // ------------------------------------------------------------ Apache's other defaults, off

  private HttpServer receiver;
  private final Map<String, AtomicInteger> hits = new ConcurrentHashMap<>();

  @AfterEach
  void stopTheReceiver() {
    if (receiver != null) {
      receiver.stop(0);
    }
  }

  /** A local receiver; {@code handler} answers each request, every hit counted by path. */
  private String receiver(HttpHandler handler) throws IOException {
    receiver = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    receiver.setExecutor(Executors.newCachedThreadPool());
    receiver.createContext(
        "/",
        exchange -> {
          hits.computeIfAbsent(exchange.getRequestURI().getPath(), path -> new AtomicInteger())
              .incrementAndGet();
          exchange.getRequestBody().readAllBytes();
          handler.handle(exchange);
          exchange.close();
        });
    receiver.start();
    return "http://127.0.0.1:" + receiver.getAddress().getPort();
  }

  private static RestClient apache(String base) {
    return RestClient.builder()
        .baseUrl(base)
        .requestFactory(OutboxHttpClients.Apache.requestFactory(CONNECT, READ, SLOTS))
        .build();
  }

  /** A receiver whose {@code /hook} answers a 302 to {@code /elsewhere}, which answers 200. */
  private String redirectingReceiver() throws IOException {
    return receiver(
        exchange -> {
          if (exchange.getRequestURI().getPath().equals("/hook")) {
            exchange.getResponseHeaders().add("Location", "/elsewhere");
            exchange.sendResponseHeaders(302, -1);
          } else {
            exchange.sendResponseHeaders(200, -1);
          }
        });
  }

  private int hitsOf(String path) {
    AtomicInteger count = hits.get(path);
    return count == null ? 0 : count.get();
  }

  /** Up to 1.11.0 Apache FOLLOWED a 3xx into a second request: the publisher saw its 2xx. */
  @Test
  void apache_neverFollowsARedirect() throws IOException {
    String base = redirectingReceiver();

    HttpStatusCode status =
        apache(base).post().uri("/hook").body("{}").retrieve().toBodilessEntity().getStatusCode();

    assertThat(status.value()).as("the 3xx reaches the publisher's own refusal").isEqualTo(302);
    assertThat(hitsOf("/elsewhere")).as("its Location never requested").isZero();
  }

  private static RestClient jetty(String base) {
    return RestClient.builder()
        .baseUrl(base)
        .requestFactory(OutboxJettyClient.requestFactory(CONNECT, READ, SLOTS))
        .build();
  }

  /** Up to 1.11.0 Jetty FOLLOWED a 3xx into a second request, as Spring's factory leaves it. */
  @Test
  void jetty_neverFollowsARedirect() throws IOException {
    String base = redirectingReceiver();

    HttpStatusCode status =
        jetty(base).post().uri("/hook").body("{}").retrieve().toBodilessEntity().getStatusCode();

    assertThat(status.value()).as("the 3xx reaches the publisher's own refusal").isEqualTo(302);
    assertThat(hitsOf("/elsewhere")).as("its Location never requested").isZero();
  }

  /** Jetty never retried a 503 or a 429 (a control: it did not before 1.11.1 either). */
  @ParameterizedTest(name = "{0}")
  @ValueSource(ints = {503, 429})
  void jetty_neverRetries(int status) throws IOException {
    String base = receiver(exchange -> exchange.sendResponseHeaders(status, -1));

    Throwable failure =
        catchThrowable(
            () -> jetty(base).post().uri("/busy").body("{}").retrieve().toBodilessEntity());

    assertThat(failure).isNotNull();
    assertThat(hitsOf("/busy")).as("one POST per attempt").isEqualTo(1);
  }

  /** Up to 1.11.0 Apache re-sent a 503 (and a 429) once inside the call: one attempt, two POSTs. */
  @ParameterizedTest(name = "{0}")
  @ValueSource(ints = {503, 429})
  void apache_neverRetries_theRelayDoes(int status) throws IOException {
    String base = receiver(exchange -> exchange.sendResponseHeaders(status, -1));

    Throwable failure =
        catchThrowable(
            () -> apache(base).post().uri("/busy").body("{}").retrieve().toBodilessEntity());

    assertThat(failure).isNotNull();
    assertThat(hitsOf("/busy")).as("one POST per attempt").isEqualTo(1);
  }

  /** Up to 1.11.0 Apache's pool let five sends per receiver host be in flight, not the lane's 8. */
  @Test
  void apache_letsEverySlotSendAtOnce_toOneReceiverHost() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger held = new AtomicInteger();
    String base =
        receiver(
            exchange -> {
              held.incrementAndGet();
              try {
                release.await(10, TimeUnit.SECONDS);
              } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
              }
              exchange.sendResponseHeaders(200, -1);
            });
    RestClient client = apache(base);
    ExecutorService slots = Executors.newFixedThreadPool(SLOTS);
    try {
      for (int n = 0; n < SLOTS; n++) {
        String path = "/hold/" + n; // one destination each, one host: Apache's route
        slots.submit(() -> client.post().uri(path).body("{}").retrieve().toBodilessEntity());
      }

      await().atMost(Duration.ofSeconds(5)).until(() -> held.get() == SLOTS);
    } finally {
      release.countDown();
      slots.shutdown();
      assertThat(slots.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }
}
