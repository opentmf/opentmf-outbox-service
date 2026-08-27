package org.opentmf.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.type.SqlTypes;

/**
 * One outbox row — an effect frozen at commit time, delivered at-least-once by the relay.
 * State is DERIVED, no status column: pending = {@code relayedOn == null && cancelledOn == null};
 * parked = pending AND {@code parkedOn != null}; relayed = {@code relayedOn != null};
 * cancelled = {@code cancelledOn != null}. A pending row whose {@code releaseAt} lies in the
 * future is HELD - not claimable until then.
 *
 * <p>Part of the library's public seam (with {@link OutboxWriter} and
 * {@link OutboxMaintenanceService}); it is also the Querydsl root of the ops list endpoint —
 * inside the LIBRARY that exposure breaks no consumer's seal.
 *
 * <p>Deliberately standalone (no audit superclass): the outbox table shape has no
 * {@code created_by}/{@code update_count} columns, and an optimistic {@code @Version} would
 * fight the relay's pessimistic {@code FOR UPDATE SKIP LOCKED} claim.
 */
@Getter
@Setter
@Entity
@DynamicInsert
@DynamicUpdate
@Table(name = "outbox")
public class OutboxEvent {

  /** Own identity — DB identity column; the relay publishes in {@code id} order. */
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  /** Aggregate kind the event belongs to, e.g. {@code party-interaction}. */
  @Column(nullable = false, updatable = false, length = 64)
  private String aggregateType;

  /** Aggregate identity — becomes the Kafka message key (preserves per-aggregate order). */
  @Column(nullable = false, updatable = false, length = 128)
  private String aggregateId;

  /** Payload event type, e.g. {@code comm.outcome.v1}; copied into {@code x-event-type}. */
  @Column(nullable = false, updatable = false, length = 100)
  private String eventType;

  /** Delivery target: a Kafka topic name, or an {@code http(s)://} URL for the HTTP publisher. */
  @Column(nullable = false, updatable = false, length = 200)
  private String destination;

  /**
   * OPTIONAL named client profile for HTTP delivery: a subscriber requiring authentication is
   * onboarded under a named client profile, and the row may select it explicitly. Null = the
   * resolver's decision (longest-prefix base-url match), else plain POST. Ignored by the Kafka
   * publisher.
   */
  @Column(updatable = false, length = 64)
  private String clientProfile;

  /** Serialized JSON payload (TEXT — a genuine blob), frozen at write time, never re-read. */
  @Column(nullable = false, updatable = false)
  private String payload;

  /**
   * Optional serialized header map, frozen at write time; TEXT. These are WIRE headers: both
   * built-in publishers forward all of them (relay-stamped names replace same-named ones).
   */
  @Column(updatable = false)
  private String headers;

  /**
   * Optional PRIVATE correlation - e.g. the subscription a hub delivery belongs to. Filterable
   * on the ops list, visible on the row view, NEVER forwarded to the wire by either built-in
   * publisher (that is what {@link #headers} is for).
   */
  @Column(updatable = false, length = 128)
  private String reference;

  /**
   * Row creation time, set by the WRITER (deliberately not {@code @CreatedDate}: a library
   * must not depend on the consumer enabling JPA auditing); feeds the relay-lag gauge.
   */
  @Column(nullable = false, updatable = false)
  private OffsetDateTime createdOn;

  /** Failed delivery attempts so far; at the publisher's budget the row parks or drops. */
  @JdbcTypeCode(SqlTypes.SMALLINT)
  @Column(nullable = false, columnDefinition = "smallint")
  private int attempts;

  /** Earliest next delivery attempt (backoff schedule); {@code now} on insert. */
  @Column(nullable = false)
  private OffsetDateTime nextAttemptOn;

  /**
   * Optional scheduled-send HOLD: the row is not claimable before this instant; null = no hold.
   * Frozen at write time ({@code updatable = false}) - the retry backoff reschedules
   * {@link #nextAttemptOn} and can structurally never move the hold.
   */
  @Column(updatable = false)
  private OffsetDateTime releaseAt;

  /**
   * Stamped when the delivery budget is exhausted with outcome PARK: the row is unclaimable
   * until {@link OutboxMaintenanceService#unpark(long)} clears it. Null while retrying.
   */
  private OffsetDateTime parkedOn;

  /**
   * Set once the effect is delivered — the row's terminal state; null while pending. Also
   * stamped by a DROP exhaustion (the row leaves the pending set; {@link #lastError} tells).
   */
  private OffsetDateTime relayedOn;

  /**
   * Cancellation time of an UNRELEASED effect - the other terminal state; null = not cancelled.
   * Set only through {@link OutboxMaintenanceService#cancel(long)}, which refuses relayed rows,
   * so relayed and cancelled never overlap.
   */
  private OffsetDateTime cancelledOn;

  /** Last delivery failure, truncated — ops forensics for parked rows. */
  private String lastError;
}
