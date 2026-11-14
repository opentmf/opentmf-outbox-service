package org.opentmf.outbox;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;

/**
 * Read-model of one outbox row for the ops surface (the no-direct-DB principle: what an
 * operator would otherwise SELECT in production rides an admin REST endpoint). {@code parked}
 * is the DERIVED state made explicit ({@code parkedOn} set and not cancelled), and so is
 * {@code inFlight} (1.3.0: a relay holds a live lease - {@code claimedUntil} in the future - on a
 * row not yet relayed); {@code releaseAt}
 * (the scheduled-send hold), {@code parkedOn}, {@code cancelledOn} and the private
 * {@code reference} ride as the raw facts. {@code payload} rides only the single-row inspect
 * (null in lists) - the list exists for triage, not bulk export; the TMF630 fields param can
 * narrow further.
 *
 * <p>With {@code opentmf.outbox.ops.sensitive-in-views=false} (1.11.1) no view carries the row's
 * sensitive text - {@code payload} is null and {@code lastError} holds only the failure's KIND, its
 * exception's type name, never its message (for an HTTP 4xx or 5xx that message holds the
 * receiver's whole response body); {@link OutboxRowPayload} serves the full text. It is in the
 * record itself, not hidden from JSON, so a {@code fields=} selection, which reads the accessors,
 * cannot bring the text back.
 */
public record OutboxRowView(
    long id,
    String aggregateType,
    String aggregateId,
    String eventType,
    String destination,
    String clientProfile,
    String reference,
    int attempts,
    boolean parked,
    boolean inFlight,
    OffsetDateTime createdOn,
    OffsetDateTime nextAttemptOn,
    OffsetDateTime releaseAt,
    OffsetDateTime parkedOn,
    OffsetDateTime claimedUntil,
    OffsetDateTime relayedOn,
    OffsetDateTime cancelledOn,
    String lastError,
    String payload) {

  /** The longest kind served; a longer leading token is no type name of the relay's. */
  static final int KIND_MAX_LENGTH = 200;

  /** The kind of a stored text not led by a throwable's name - never a piece of the text. */
  static final String UNCLASSIFIED = "unclassified";

  /**
   * Spring's HTTP-status exceptions whose simple names end neither in {@code Exception} nor in
   * {@code Error} ({@code BadRequest}, {@code ServiceUnavailable}, ...): the relay stores a 4xx or
   * 5xx under that name. Read from spring-web where it is on the classpath - by name, so the view
   * does not need it - and empty where it is not: there the relay writes no such name.
   */
  private static final Set<String> SPRING_STATUS_EXCEPTIONS =
      nestedThrowables(
          "org.springframework.web.client.HttpClientErrorException",
          "org.springframework.web.client.HttpServerErrorException");

  static OutboxRowView of(OutboxEvent event, boolean withPayload) {
    return of(event, withPayload, true, OffsetDateTime.now(ZoneOffset.UTC));
  }

  /** The view as of {@code now} - the outbox's time, which decides whether a lease is live. */
  static OutboxRowView of(OutboxEvent event, boolean withPayload, OffsetDateTime now) {
    return of(event, withPayload, true, now);
  }

  /**
   * The view as of {@code now}; {@code sensitive} false leaves the payload out and keeps only the
   * failure's kind in {@code lastError}.
   */
  static OutboxRowView of(
      OutboxEvent event, boolean withPayload, boolean sensitive, OffsetDateTime now) {
    return new OutboxRowView(
        event.getId(),
        event.getAggregateType(),
        event.getAggregateId(),
        event.getEventType(),
        event.getDestination(),
        event.getClientProfile(),
        event.getReference(),
        event.getAttempts(),
        event.getParkedOn() != null && event.getCancelledOn() == null,
        event.getRelayedOn() == null
            && event.getClaimedUntil() != null
            && event.getClaimedUntil().isAfter(now),
        event.getCreatedOn(),
        event.getNextAttemptOn(),
        event.getReleaseAt(),
        event.getParkedOn(),
        event.getClaimedUntil(),
        event.getRelayedOn(),
        event.getCancelledOn(),
        sensitive ? event.getLastError() : kindOf(event.getLastError()),
        withPayload && sensitive ? event.getPayload() : null);
  }

  /**
   * The failure's kind: the stored text's leading throwable name, ended by {@code :} or the end, at
   * most {@link #KIND_MAX_LENGTH} characters; {@link #UNCLASSIFIED} for any other text, and null
   * for none. A type name's shape is not enough: a receiver's word, a person's name or a dotted
   * name before a colon has it too. So the name's last segment must name a throwable as Java names
   * them - ending in {@code Exception} or {@code Error} - or be one of Spring's HTTP-status
   * exceptions (1.11.1). A consumer's own exception named otherwise shows as {@link #UNCLASSIFIED};
   * its full text is on the payload path.
   */
  static String kindOf(String lastError) {
    if (lastError == null) {
      return null;
    }
    int colon = lastError.indexOf(':');
    String token = colon < 0 ? lastError : lastError.substring(0, colon);
    return isTypeName(token) && namesAThrowable(token) ? token : UNCLASSIFIED;
  }

  /** Whether a type name's last segment, after its last dot or dollar, is a throwable's. */
  private static boolean namesAThrowable(String typeName) {
    String simple =
        typeName.substring(Math.max(typeName.lastIndexOf('.'), typeName.lastIndexOf('$')) + 1);
    return simple.endsWith("Exception")
        || simple.endsWith("Error")
        || SPRING_STATUS_EXCEPTIONS.contains(simple);
  }

  /** The simple names of the throwables nested in these classes; none for a class not present. */
  private static Set<String> nestedThrowables(String... classNames) {
    Set<String> names = new HashSet<>();
    for (String className : classNames) {
      try {
        Class<?> outer = Class.forName(className, false, OutboxRowView.class.getClassLoader());
        for (Class<?> nested : outer.getDeclaredClasses()) {
          if (Throwable.class.isAssignableFrom(nested)) {
            names.add(nested.getSimpleName());
          }
        }
      } catch (ClassNotFoundException absent) {
        // not on this consumer's classpath: nor is the HTTP publisher that would store the name
      }
    }
    return Set.copyOf(names);
  }

  /**
   * A Java type name's shape, in ASCII: identifier segments - a letter, {@code _} or {@code $},
   * then letters, digits, {@code _} or {@code $} - joined by single dots, at most {@link
   * #KIND_MAX_LENGTH} characters. Read in one pass, no pattern: a stored text is up to 4,000
   * characters of anything.
   */
  private static boolean isTypeName(String token) {
    if (token.isEmpty() || token.length() > KIND_MAX_LENGTH) {
      return false;
    }
    boolean segmentStart = true;
    for (int i = 0; i < token.length(); i++) {
      char c = token.charAt(i);
      if (c == '.' && !segmentStart) {
        segmentStart = true;
      } else if (identifierStart(c) || (!segmentStart && digit(c))) {
        segmentStart = false;
      } else {
        return false;
      }
    }
    return !segmentStart;
  }

  private static boolean identifierStart(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '$';
  }

  private static boolean digit(char c) {
    return c >= '0' && c <= '9';
  }
}
