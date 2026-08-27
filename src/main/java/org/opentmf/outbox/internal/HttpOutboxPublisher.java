package org.opentmf.outbox.internal;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.opentmf.outbox.OutboxClientProfileResolver;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxHeaders;
import org.opentmf.outbox.OutboxPublisher;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/**
 * The HTTP publisher: {@code http(s)://} destinations are POSTed the row's payload as JSON,
 * with the relay headers as HTTP headers. Client selection: an
 * explicit per-row {@code clientProfile} - or, absent one, the consumer's
 * {@link OutboxClientProfileResolver} may match the destination by base-url longest prefix;
 * no resolver or no match = the plain default client (a subscriber needing auth is onboarded
 * under a named profile, existing plain subscribers stay untouched).
 *
 * <p>Stored headers are forwarded first; the relay headers are then SET (replacing a stored
 * header of the same name - the Kafka leg does the same). The row's {@code reference} is
 * never sent.
 *
 * <p>Non-2xx answers throw (RestClient's default error handling), unwinding into the relay's
 * ordinary backoff-then-park bookkeeping.
 */
@Slf4j
class HttpOutboxPublisher implements OutboxPublisher {

  private final RestClient plainClient;
  private final OutboxClientProfileResolver profileResolver;
  private final ObjectMapper objectMapper;
  private final String serviceName;

  HttpOutboxPublisher(
      RestClient plainClient,
      OutboxClientProfileResolver profileResolver,
      ObjectMapper objectMapper,
      String serviceName) {
    this.plainClient = plainClient;
    this.profileResolver = profileResolver;
    this.objectMapper = objectMapper;
    this.serviceName = serviceName;
  }

  @Override
  public boolean supports(OutboxEvent event) {
    String destination = event.getDestination();
    return destination.startsWith("http://") || destination.startsWith("https://");
  }

  @Override
  public void publish(OutboxEvent event) {
    RestClient client = selectClient(event);
    Map<String, String> stored = storedHeaders(event);
    client
        .post()
        .uri(event.getDestination())
        .contentType(MediaType.APPLICATION_JSON)
        .headers(
            headers -> {
              stored.forEach(headers::set);
              headers.set(
                  OutboxHeaders.IDEMPOTENCY_KEY,
                  OutboxHeaders.idempotencyKey(serviceName, event.getId()));
              headers.set(OutboxHeaders.EVENT_TYPE, event.getEventType());
              headers.set(OutboxHeaders.PRODUCER, serviceName);
            })
        .body(event.getPayload())
        .retrieve()
        .toBodilessEntity();
  }

  private RestClient selectClient(OutboxEvent event) {
    if (profileResolver == null) {
      return plainClient;
    }
    RestClient resolved =
        profileResolver.resolve(event.getClientProfile(), event.getDestination());
    return resolved != null ? resolved : plainClient;
  }

  private Map<String, String> storedHeaders(OutboxEvent event) {
    if (event.getHeaders() == null) {
      return Map.of();
    }
    return objectMapper.readValue(event.getHeaders(), new TypeReference<Map<String, String>>() {});
  }
}
