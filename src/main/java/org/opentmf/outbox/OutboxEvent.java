package org.opentmf.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
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
import org.opentmf.outbox.OutboxPublisher.Lane;

/**
 * One outbox row — an effect frozen at commit time, delivered at-least-once by the relay.
 * State is DERIVED, no status column: pending = {@code relayedOn == null && cancelledOn == null};
 * parked = pending AND {@code parkedOn != null}; relayed = {@code relayedOn != null};
 * cancelled = {@code cancelledOn != null}. A pending row whose {@code releaseAt} lies in the
 * future is HELD - not claimable until then; a pending row whose {@code claimedUntil} lies in the
 * future is IN FLIGHT - a relay holds its lease (1.3.0). Relayed and cancelled overlap only for a
 * row cancelled after its lease lapsed mid-send, and then delivered (sent-but-cancelled).
 *
 * <p>Part of the library's public seam (with {@link OutboxWriter} and
 * {@link OutboxMaintenanceService}); it is also the Querydsl root of the ops list endpoint —
 * inside the LIBRARY that exposure breaks no consumer's seal.
 *
 * <p>Deliberately standalone (no audit superclass): the outbox table shape has no
 * {@code created_by}/{@code update_count} columns, and an optimistic {@code @Version} would
 * fight the relay's pessimistic claim and its lease-guarded bookings.
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
   * Set only through {@link OutboxMaintenanceService#cancel(long)}, which refuses relayed rows
   * and rows under a live lease. A row cancelled after its lease lapsed mid-send, and then
   * delivered, carries both stamps (sent-but-cancelled) - the only overlap.
   */
  private OffsetDateTime cancelledOn;

  /**
   * The relay's LEASE on the row (1.3.0): stamped {@code now + lease} by the claim, cleared by
   * the booking. While it lies in the future the row is in flight and not claimable; once it
   * lapses unbooked (a crash, a call longer than the lease) any relay may claim the row again,
   * and the late holder's booking - guarded on the value it stamped - books nothing.
   */
  private OffsetDateTime claimedUntil;

  /**
   * The relay lane the row's publisher named at APPEND (1.3.0), frozen - the stamped lane wins
   * at claim even if the publisher would now say otherwise. Null = written before 1.3.0 or not
   * through {@link OutboxWriter}: the row rides ORDERED.
   */
  @Enumerated(EnumType.STRING)
  @Column(updatable = false, length = 16)
  private Lane lane;

  /**
   * CONCURRENT lane (1.3.0): rows sharing this key are never in flight together, within a pod and
   * across pods, and are taken in {@code id} order on the happy path; null = independent.
   * Frozen at append; a key over 255 characters is stored as {@code sha256:<hex>}.
   */
  @Column(updatable = false)
  private String orderingKey;

  /** Last delivery failure, truncated — ops forensics for parked rows. */
  private String lastError;
}
