package org.opentmf.outboxinvariants;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.opentmf.outbox.OutboxHeaders;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.opentmf.outboxinvariants.Receiver.Delivery;
import org.opentmf.outboxinvariants.Receiver.Outcome;

/**
 * An in-process HTTP receiver for the library's own HTTP publisher: per path, an answer (status,
 * body, delay, a slow-loris that sends its headers and never the body). Every request it finishes
 * is recorded as a delivery (the row id from the idempotency key) with its headers.
 */
final class HttpReceiver implements AutoCloseable {

  /** What one request to a path gets. */
  record Answer(
      int status, String body, Duration delay, boolean slowLoris, Map<String, String> headers) {
    static Answer ok() {
      return new Answer(200, "", Duration.ZERO, false, Map.of());
    }

    static Answer status(int status) {
      return new Answer(status, "", Duration.ZERO, false, Map.of());
    }
  }

  /** One request as received. */
  record Request(String path, Map<String, List<String>> headers, Instant at) {}

  private final HttpServer server;
  private final Map<String, Function<Integer, Answer>> answers = new ConcurrentHashMap<>();
  private final Map<String, AtomicInteger> hits = new ConcurrentHashMap<>();
  final List<Request> requests = new CopyOnWriteArrayList<>();

  HttpReceiver() {
    try {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
    server.setExecutor(
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r, "http-receiver");
              t.setDaemon(true);
              return t;
            }));
    server.createContext("/", this::handle);
    server.start();
  }

  /** The destination URL of a path. */
  String url(String path) {
    return "http://127.0.0.1:" + server.getAddress().getPort() + path;
  }

  /** Request n (1-based) to {@code path} gets {@code answer.apply(n)}. */
  void answer(String path, Function<Integer, Answer> answer) {
    answers.put(path, answer);
  }

  int hits(String path) {
    return hits.getOrDefault(path, new AtomicInteger()).get();
  }

  private void handle(HttpExchange exchange) throws IOException {
    Instant start = Instant.now();
    String path = exchange.getRequestURI().getPath();
    exchange.getRequestBody().readAllBytes();
    requests.add(new Request(path, Map.copyOf(exchange.getRequestHeaders()), start));
    int n = hits.computeIfAbsent(path, p -> new AtomicInteger()).incrementAndGet();
    Answer answer = answers.getOrDefault(path, i -> Answer.ok()).apply(n);
    String key = exchange.getRequestHeaders().getFirst(OutboxHeaders.IDEMPOTENCY_KEY);
    if (key != null) {
      Receiver.started(Long.parseLong(key.substring(key.lastIndexOf(':') + 1)));
    }
    boolean interrupted = false;
    try {
      answer.headers().forEach((k, v) -> exchange.getResponseHeaders().add(k, v));
      if (answer.slowLoris()) { // the headers, then nothing
        exchange.sendResponseHeaders(answer.status(), 1_000_000);
        exchange.getResponseBody().flush();
        Waits.interruptibly(answer.delay());
        return;
      }
      Waits.interruptibly(answer.delay());
      byte[] body = answer.body().getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(answer.status(), body.length == 0 ? -1 : body.length);
      if (body.length > 0) {
        try (OutputStream out = exchange.getResponseBody()) {
          out.write(body);
        }
      }
    } catch (InterruptedException ex) {
      interrupted = true;
      Thread.currentThread().interrupt();
    } catch (IOException ex) {
      interrupted = true; // the client went away
    } finally {
      exchange.close();
      if (key != null) {
        long rowId = Long.parseLong(key.substring(key.lastIndexOf(':') + 1));
        // a 2xx status line is the receiver's acceptance, whether or not it ever sends the body
        boolean ok = answer.status() >= 200 && answer.status() < 300;
        Receiver.recordDelivery(
            new Delivery(
                rowId,
                url(path),
                url(path),
                Lane.CONCURRENT,
                "http",
                n,
                start,
                Instant.now(),
                ok ? Outcome.OK : Outcome.FAIL,
                interrupted));
      }
    }
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
