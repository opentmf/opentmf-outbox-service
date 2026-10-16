package org.opentmf.outbox.internal;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.opentmf.outbox.OutboxClientProfileResolver;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxHeaders;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.TerminalOutboxException;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
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
 * <p>Only a 2xx relays. A 4xx or 5xx throws (RestClient's default error handling) and a 3xx
 * throws too (1.9.0 - booked RELAYED before, though the receiver never processed the request);
 * a redirect is never followed into a second POST. Each unwinds into the relay's ordinary
 * backoff-then-park bookkeeping.
 *
 * <p>A row that NAMES a client profile the resolver does not know (or with no resolver at all)
 * is refused with a {@link TerminalOutboxException} naming the profile, and parks at once (1.9.0 -
 * it went out through the plain client before, without the profile's authentication). A row that
 * names none keeps the base-url match and the plain fallback. The plain client has a connect
 * and a read timeout ({@code opentmf.outbox.http.*}, 1.9.0; none before).
 *
 * <p>Lane (1.3.0): CONCURRENT - an HTTP row never waits behind another receiver, and never holds
 * up an ORDERED row. The ordering key is the DESTINATION, so rows to one receiver are never in
 * flight together and go in {@code id} order on the happy path, as they did on the single relay
 * thread.
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
  public Lane lane(OutboxEvent event) {
    return Lane.CONCURRENT;
  }

  /** Per-receiver order: the destination URL is the key. */
  @Override
  public String orderingKey(OutboxEvent event) {
    return event.getDestination();
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
        .onStatus(
            status -> status.is3xxRedirection(),
            (request, response) -> {
              throw new RestClientResponseException(
                  "%s from %s - only a 2xx relays; a redirect is not followed (Location: %s)"
                      .formatted(
                          response.getStatusCode(),
                          event.getDestination(),
                          response.getHeaders().getFirst("Location")),
                  response.getStatusCode(),
                  response.getStatusText(),
                  response.getHeaders(),
                  null,
                  null);
            })
        .toBodilessEntity();
  }

  private RestClient selectClient(OutboxEvent event) {
    String profile = event.getClientProfile();
    if (profileResolver == null) {
      if (profile != null) {
        throw new TerminalOutboxException(
            "Outbox row %d names the client profile '%s', and no OutboxClientProfileResolver"
                    .formatted(event.getId(), profile)
                + " is configured - not sent through the plain client");
      }
      return plainClient;
    }
    RestClient resolved = profileResolver.resolve(profile, event.getDestination());
    if (resolved == null && profile != null) {
      throw new TerminalOutboxException(
          "Outbox row %d names the client profile '%s', which the OutboxClientProfileResolver"
                  .formatted(event.getId(), profile)
              + " does not know - not sent through the plain client");
    }
    return resolved != null ? resolved : plainClient;
  }

  private Map<String, String> storedHeaders(OutboxEvent event) {
    if (event.getHeaders() == null) {
      return Map.of();
    }
    return objectMapper.readValue(event.getHeaders(), new TypeReference<Map<String, String>>() {});
  }
}
