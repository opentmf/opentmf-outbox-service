package org.opentmf.outboxinvariants;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxAppend;
import org.opentmf.outbox.OutboxHeaders;
import org.opentmf.outboxinvariants.HttpReceiver.Answer;
import org.opentmf.outboxinvariants.HttpReceiver.Request;

/** F - the library's own HTTP publisher against a programmable receiver. */
class HttpIT {

  private static final String[] HTTP = {
    "it.http.profiles=true",
    "opentmf.outbox.max-attempts=3",
    "opentmf.outbox.backoff-base=200ms",
    "opentmf.outbox.backoff-cap=1s"
  };

  private static OutboxAppend row(String url, String aggregate) {
    return OutboxAppend.of("it", aggregate, "it.http.v1", url, Map.of("a", aggregate));
  }

  /**
   * F1: a row naming a client profile the resolver does not know - parked with the reason, never
   * sent through the plain client (no auth); a known profile is used.
   */
  @Test
  void f1_anUnknownClientProfile_parksWithTheReason() {
    try (Scenario s = Scenario.start("F1-unknown-client-profile", 1, HTTP);
        HttpReceiver http = new HttpReceiver()) {
      long known = s.pod(0).append(row(http.url("/f1/known"), "known").withClientProfile("good"));
      long unknown =
          s.pod(0).append(row(http.url("/f1/unknown"), "unknown").withClientProfile("nope"));
      Findings f = s.finish(Duration.ofSeconds(60), Expect.strict());
      boolean knownUsedProfile =
          http.requests.stream()
              .anyMatch(r -> r.path().equals("/f1/known") && "good".equals(first(r, "X-Client")));
      if (!knownUsedProfile) {
        f.add("S", "a row of a known profile was not sent through the profile's client");
      }
      if (http.hits("/f1/unknown") > 0) {
        f.add(
            "S",
            "a row naming an UNKNOWN client profile was sent through the plain client (%d requests) - expected: parked with the reason"
                .formatted(http.hits("/f1/unknown")));
      } else if (Scenario.count(
              "id = " + unknown + " and parked_on is not null and last_error like '%profile%'")
          != 1) {
        f.add("S", "the unknown-profile row is not parked with a reason naming the profile");
      }
      f.figure("unknown-profile requests=%d known=%d".formatted(http.hits("/f1/unknown"), known));
      f.assertNone();
    }
  }

  /** F2: a 401 once, on a profile whose client re-authenticates - relayed in ONE attempt. */
  @Test
  void f2_a401Once_reAuthenticates_relayedInOneAttempt() {
    try (Scenario s = Scenario.start("F2-401-reauth", 1, HTTP);
        HttpReceiver http = new HttpReceiver()) {
      http.answer("/f2", n -> n == 1 ? Answer.status(401) : Answer.ok());
      long id = s.pod(0).append(row(http.url("/f2"), "f2").withClientProfile("auth"));
      Findings f = s.finish(Duration.ofSeconds(60), Expect.strict().mayDuplicate(id));
      if (Scenario.count("id = " + id + " and relayed_on is not null and attempts = 0") != 1) {
        f.add("S", "a 401 the client re-authenticated past was not relayed in one attempt");
      }
      List<String> tokens =
          http.requests.stream()
              .filter(r -> r.path().equals("/f2"))
              .map(r -> first(r, "Authorization"))
              .toList();
      f.figure("authorizations seen: " + tokens);
      f.assertNone();
    }
  }

  /**
   * F3: a stored header named like a client default header - ONE header arrives, the stored one; a
   * stored header named like a relay header - ONE, the relay's.
   */
  @Test
  void f3_storedHeaderVsClientHeader_oneHeaderTheStoredOne() {
    try (Scenario s = Scenario.start("F3-header-collision", 1, HTTP);
        HttpReceiver http = new HttpReceiver()) {
      s.pod(0)
          .append(
              row(http.url("/f3"), "f3")
                  .withClientProfile("good")
                  .withHeaders(
                      Map.of("X-Tenant", "stored", OutboxHeaders.IDEMPOTENCY_KEY, "forged")));
      Findings f = s.finish(Duration.ofSeconds(60), Expect.strict());
      await().atMost(Duration.ofSeconds(5)).until(() -> !http.requests.isEmpty());
      Request request = http.requests.get(0);
      List<String> tenant = values(request, "X-Tenant");
      List<String> key = values(request, OutboxHeaders.IDEMPOTENCY_KEY);
      f.figure("X-Tenant=" + tenant + " x-idempotency-key=" + key);
      if (!tenant.equals(List.of("stored"))) {
        f.add("S", "the stored X-Tenant should arrive alone, arrived as " + tenant);
      }
      if (key.size() != 1 || key.get(0).equals("forged")) {
        f.add("S", "the relay's idempotency key should replace a stored one, arrived as " + key);
      }
      f.assertNone();
    }
  }

  /**
   * F4: a slow-loris receiver (the status line and headers, then nothing) - the send ends within
   * the send timeout and counts as a failed attempt; it never holds a slot for as long as the
   * receiver likes. Two pods, a five-second lease.
   */
  @Test
  void f4_aSlowLorisReceiver_timesOut_andCountsAnAttempt() {
    try (Scenario s =
            Scenario.start(
                "F4-slow-loris",
                2,
                "it.http.profiles=true",
                "opentmf.outbox.lease=5s",
                "opentmf.outbox.send-timeout=2s",
                "opentmf.outbox.max-attempts=5",
                "opentmf.outbox.backoff-base=200ms",
                "opentmf.outbox.backoff-cap=1s");
        HttpReceiver http = new HttpReceiver()) {
      // request 1 never answers (12 s, then the status); request 2 sends its headers and stalls the
      // body
      http.answer(
          "/f4/silent",
          n -> n == 1 ? new Answer(200, "", Duration.ofSeconds(12), false, Map.of()) : Answer.ok());
      http.answer(
          "/f4/loris",
          n ->
              n == 1 ? new Answer(200, "{}", Duration.ofSeconds(12), true, Map.of()) : Answer.ok());
      long silent = s.pod(0).append(row(http.url("/f4/silent"), "f4-silent"));
      long loris = s.pod(0).append(row(http.url("/f4/loris"), "f4-loris"));
      Findings f =
          s.finish(Duration.ofMinutes(2), Expect.strict().mayDuplicate(silent).mayDuplicate(loris));
      for (long id : List.of(silent, loris)) {
        String path = id == silent ? "/f4/silent" : "/f4/loris";
        f.figure(
            "%s: requests=%d, attempts booked=%d, last_error=%s"
                .formatted(
                    path,
                    http.hits(path),
                    Scenario.count("id = " + id + " and attempts > 0"),
                    Scenario.sql1("select last_error from outbox where id = " + id)));
      }
      // silent: the read timeout fails the attempt (and the retry relays), never a takeover
      if (Scenario.count("id = " + silent + " and attempts > 0") == 0) {
        f.add(
            "S",
            "/f4/silent: a receiver that held the request 12 s never failed an attempt - no read timeout on the HTTP client (%d requests, the lease 5 s)"
                .formatted(http.hits("/f4/silent")));
      } else if (http.hits("/f4/silent") > 2) {
        f.add(
            "S",
            "/f4/silent: %d requests - the held request was also taken over"
                .formatted(http.hits("/f4/silent")));
      }
      // loris: its 200 status line is the acceptance - relayed at once, the stalled body unread
      if (Scenario.count("id = " + loris + " and relayed_on is not null and attempts = 0") != 1
          || http.hits("/f4/loris") != 1) {
        f.add(
            "S",
            "/f4/loris: a 200 whose body stalls was not relayed at once on its status (%d requests)"
                .formatted(http.hits("/f4/loris")));
      }
      f.assertNone();
    }
  }

  /** F5: 200 with a body, 204, 202 - relayed; a 302 - not followed into a second POST. */
  @Test
  void f5_2xxVariantsRelay_a3xxIsNeverFollowedIntoASecondPost() {
    try (Scenario s = Scenario.start("F5-2xx-and-3xx", 1, HTTP);
        HttpReceiver http = new HttpReceiver()) {
      http.answer(
          "/f5/200",
          n ->
              new Answer(
                  200,
                  "{\"ok\":true}",
                  Duration.ZERO,
                  false,
                  Map.of("Content-Type", "application/json")));
      http.answer("/f5/204", n -> Answer.status(204));
      http.answer("/f5/202", n -> Answer.status(202));
      http.answer(
          "/f5/302",
          n ->
              new Answer(
                  302, "", Duration.ZERO, false, Map.of("Location", http.url("/f5/target"))));
      long r200 = s.pod(0).append(row(http.url("/f5/200"), "200"));
      long r204 = s.pod(0).append(row(http.url("/f5/204"), "204"));
      long r202 = s.pod(0).append(row(http.url("/f5/202"), "202"));
      long r302 = s.pod(0).append(row(http.url("/f5/302"), "302"));
      Findings f = s.finish(Duration.ofSeconds(60), Expect.strict());
      for (long id : List.of(r200, r204, r202)) {
        if (Scenario.count("id = " + id + " and relayed_on is not null and attempts = 0") != 1) {
          f.add("S", "a 2xx row %d was not relayed at the first attempt".formatted(id));
        }
      }
      if (http.hits("/f5/target") > 0) {
        f.add(
            "S",
            "the 302 was followed: %d request(s) to its Location"
                .formatted(http.hits("/f5/target")));
      }
      if (Scenario.count("id = " + r302 + " and relayed_on is not null and parked_on is null")
          == 1) {
        f.add(
            "S",
            "a 302 was booked RELAYED - the receiver never processed the request (the publisher's"
                + " javadoc says non-2xx throws)");
      }
      f.assertNone();
    }
  }

  private static String first(Request r, String header) {
    List<String> v = values(r, header);
    return v.isEmpty() ? null : v.get(0);
  }

  private static List<String> values(Request r, String header) {
    return r.headers().entrySet().stream()
        .filter(e -> e.getKey().equalsIgnoreCase(header))
        .flatMap(e -> e.getValue().stream())
        .toList();
  }
}
