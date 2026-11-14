package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxLeaseLapsedException;
import org.opentmf.outbox.OutboxRowView;
import org.opentmf.outbox.OutboxSendError;
import org.opentmf.outbox.TerminalOutboxException;
import org.opentmf.outbox.internal.KafkaLessStartupTests.ConsumerClassLoader;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.KafkaException;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;

/**
 * The kinds the relay itself writes (1.11.1): every failure it books, through its own {@code
 * describe}, is shown as its type name in a view without the sensitive text - never as {@code
 * unclassified}. A change to how the relay describes a failure that would hide a real kind is red
 * here.
 */
class OutboxRelayKindsTests {

  @Test
  void everyFailureTheRelayBooks_showsItsTypeName() throws Exception {
    List<RuntimeException> failures = new ArrayList<>();
    failures.add(new KafkaException("Failed to publish outbox row 7 to t"));
    failures.add(new TimeoutException("Expiring 1 record(s) for t-0"));
    failures.add(new InterruptException("interrupted at the send bound"));
    failures.add(new TerminalOutboxException("Outbox row 7 names the client profile 'p'"));
    failures.add(new OutboxLeaseLapsedException("the lease of the send before this claim lapsed"));
    failures.add(new OutboxSendError(new OutOfMemoryError("Java heap space")));
    failures.add(new ResourceAccessException("I/O error on POST request for \"https://hub/cb\""));
    failures.add(
        new RestClientResponseException(
            "302 FOUND from https://hub/cb - only a 2xx relays",
            302,
            "Found",
            new HttpHeaders(),
            null,
            null));
    failures.add(new IllegalStateException("No OutboxPublisher supports destination 'x'"));
    failures.add(new IllegalStateException()); // no message: its toString, the binary name
    for (HttpStatus status : HttpStatus.values()) {
      if (status.is4xxClientError()) {
        failures.add(HttpClientErrorException.create(status, "x", new HttpHeaders(), null, null));
      } else if (status.is5xxServerError()) {
        failures.add(HttpServerErrorException.create(status, "x", new HttpHeaders(), null, null));
      }
    }
    Method kindOf = kindOf(OutboxRowView.class);
    for (RuntimeException failure : failures) {
      String expected =
          failure.getMessage() == null
              ? failure.getClass().getName()
              : failure.getClass().getSimpleName();
      assertThat(kindOf.invoke(null, OutboxRelayWorker.describe(failure)))
          .as("the kind of %s", failure.getClass().getName())
          .isEqualTo(expected);
    }
  }

  @Test
  void withoutSpringWeb_theViewLoads_andTheThrowableNamesStillShow() throws Exception {
    try (ConsumerClassLoader withoutWeb = new ConsumerClassLoader("org.springframework.web.")) {
      Class<?> view = withoutWeb.loadClass(OutboxRowView.class.getName());
      assertThat(view.getClassLoader()).isSameAs(withoutWeb);
      Method kindOf = kindOf(view);

      assertThat(kindOf.invoke(null, "KafkaException: Failed to publish"))
          .isEqualTo("KafkaException");
      // Spring's status names are known only where spring-web is - and only it writes them
      assertThat(kindOf.invoke(null, "BadRequest: 400 Bad Request")).isEqualTo("unclassified");
      assertThat(kindOf.invoke(null, "boom")).isEqualTo("unclassified");
    }
  }

  private static Method kindOf(Class<?> view) throws NoSuchMethodException {
    Method kindOf = view.getDeclaredMethod("kindOf", String.class);
    kindOf.setAccessible(true);
    return kindOf;
  }
}
