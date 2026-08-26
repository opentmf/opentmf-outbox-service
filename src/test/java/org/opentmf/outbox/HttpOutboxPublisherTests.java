package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import tools.jackson.databind.ObjectMapper;

/** POSTs the payload with S14 headers; profile selection; non-2xx unwinds to the relay. */
class HttpOutboxPublisherTests {

  private static OutboxEvent event(String destination, String profile) {
    OutboxEvent event = new OutboxEvent();
    event.setId(9L);
    event.setAggregateId("agg-9");
    event.setEventType("hub.event.v1");
    event.setDestination(destination);
    event.setClientProfile(profile);
    event.setPayload("{\"n\":1}");
    return event;
  }

  @Test
  void supports_onlyHttpDestinations() {
    RestClient plain = RestClient.create();
    HttpOutboxPublisher publisher =
        new HttpOutboxPublisher(plain, null, new ObjectMapper(), "svc");
    assertThat(publisher.supports(event("https://hub/cb", null))).isTrue();
    assertThat(publisher.supports(event("comm.delivery.v1", null))).isFalse();
  }

  @Test
  void publish_postsPayloadWithRelayHeaders_viaThePlainClient() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    server
        .expect(requestTo("https://hub/cb"))
        .andExpect(method(HttpMethod.POST))
        .andExpect(header("x-idempotency-key", "svc:outbox:9"))
        .andExpect(header("x-event-type", "hub.event.v1"))
        .andExpect(header("x-producer", "svc"))
        .andRespond(withSuccess());

    new HttpOutboxPublisher(builder.build(), null, new ObjectMapper(), "svc")
        .publish(event("https://hub/cb", null));

    server.verify();
  }

  @Test
  void publish_prefersTheResolvedNamedClient() {
    RestClient.Builder named = RestClient.builder();
    MockRestServiceServer namedServer = MockRestServiceServer.bindTo(named).build();
    namedServer.expect(requestTo("https://hub/cb")).andRespond(withSuccess());
    RestClient namedClient = named.build();

    OutboxClientProfileResolver resolver =
        (profile, destination) -> "hub-7".equals(profile) ? namedClient : null;

    new HttpOutboxPublisher(RestClient.create(), resolver, new ObjectMapper(), "svc")
        .publish(event("https://hub/cb", "hub-7"));

    namedServer.verify();
  }

  @Test
  void publish_sendsStoredHeaders_relayStampedOnesWin() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    server
        .expect(requestTo("https://hub/cb"))
        .andExpect(header("x-schema-version", "2"))
        .andRespond(withSuccess());

    OutboxEvent event = event("https://hub/cb", null);
    event.setHeaders("{\"x-schema-version\":\"2\"}");
    new HttpOutboxPublisher(builder.build(), null, new ObjectMapper(), "svc").publish(event);

    server.verify();
  }

  @Test
  void publish_resolverWithoutAMatch_fallsBackToThePlainClient() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer plainServer = MockRestServiceServer.bindTo(builder).build();
    plainServer.expect(requestTo("https://hub/cb")).andRespond(withSuccess());

    OutboxClientProfileResolver noMatch = (profile, destination) -> null;
    new HttpOutboxPublisher(builder.build(), noMatch, new ObjectMapper(), "svc")
        .publish(event("https://hub/cb", "unknown-profile"));

    plainServer.verify();
  }

  @Test
  void publish_non2xx_throwsForTheRelay() {
    RestClient.Builder builder = RestClient.builder();
    MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
    server
        .expect(requestTo("https://hub/cb"))
        .andRespond(withStatus(HttpStatus.SERVICE_UNAVAILABLE));

    HttpOutboxPublisher publisher =
        new HttpOutboxPublisher(builder.build(), null, new ObjectMapper(), "svc");

    assertThatExceptionOfType(RestClientResponseException.class)
        .isThrownBy(() -> publisher.publish(event("https://hub/cb", null)));
  }
}
