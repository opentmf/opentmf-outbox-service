# Changelog

## 1.3.0 - 2026-10-06

OUTBOX-HTTP-LANE-1: HTTP rows leave the ordered relay, and every row is claimed
by **lease**. Until now the relay was one thread that published up to
`batch-size` rows one after another inside ONE transaction. A slow HTTP
subscriber delayed every Kafka row behind it, and a database connection plus
the batch's row locks were held across every backend call.

### Changed (behaviour - read before upgrading)

- **A publisher's own database write moves from `publish` to the booking
  hook.** `publish` now runs in NO transaction. The 1.1.0 contract (a write
  inside `publish` commits with `relayed_on`, the claim transaction) is
  replaced by `OutboxPublisher.onBooked(event, OutboxBooking)`. The hook runs
  inside the short booking transaction for EVERY outcome: `RELAYED` with the
  result `deliver` returned, `RETRY`, `EXHAUSTED` (PARK/DROP), `CANCELLED`. Its
  writes commit with the row's bookkeeping and roll back with it.
  `OutboxRelayedListener.onRelayed` keeps its guarantee on both lanes: it runs
  in the transaction that stamps `relayed_on`, after the hook.
- **HTTP rows are no longer in `id` order against other rows.** The library's
  `HttpOutboxPublisher` rides the new CONCURRENT lane. An HTTP row keeps no
  order against Kafka (ORDERED) rows, nor against HTTP rows to other
  receivers. Rows to the SAME receiver keep their `id` order on the happy path
  (the destination is the ordering key; a failed row backs off, and later rows
  may pass it).
- **Cancel of a row in flight succeeds at once.** It used to block behind the
  claim and then refuse with "already relayed". The booking honours the
  cancel: a send that fails retires the row cancelled (no retry, no park), and
  a send that succeeds is booked **sent-but-cancelled**. Both `relayed_on` and
  `cancelled_on` are set, the row is listed under both states, and the relayed
  listeners fire, because the effect did leave.
- **A crash between send and booking redelivers once the lease lapses**, not
  on the next pass: up to 15 s on the ORDERED lane, up to the CONCURRENT
  lease (2 min by default).
- A booking refused by a hook or listener now books one failed attempt
  (`attempts++`). The send ran outside the booking, so the rollback cannot
  pretend the attempt never happened.

### Added

- **Two lanes, chosen by the publisher:** `OutboxPublisher.lane(event)` is
  `ORDERED` (the default: the single relay thread, `id` order) or `CONCURRENT`
  (parallel sends, at most `opentmf.outbox.concurrent.max-in-flight`, default
  8, per pod). The Kafka publisher stays ORDERED and the HTTP publisher is
  CONCURRENT.
- **Ordering key** for the CONCURRENT lane: `OutboxPublisher.orderingKey(event)`.
  Rows sharing a non-null key are never in flight together and are taken in
  `id` order.
- **Claim by lease.** New nullable column `claimed_until`. A short claim
  transaction stamps `now + lease` and commits, the send runs in no
  transaction and holds no database connection, and a short booking
  transaction writes the outcome, guarded on the stamped value. A holder whose
  lease lapsed books nothing. The lease is per publisher
  (`OutboxPublisher.lease(event)`), with lane defaults `opentmf.outbox.lease`
  (2 min, CONCURRENT) and `opentmf.outbox.ordered.lease` (15 s, ORDERED;
  validated to exceed `send-timeout`). Each ORDERED row's lease is renewed by
  the booking of the row before it, so it covers one send.
- **Virtual threads where available.** The CONCURRENT lane runs on Spring's
  `SimpleAsyncTaskExecutor`: virtual threads on a JDK 21+ runtime with
  `spring.threads.virtual.enabled=true`, platform threads otherwise. The claim
  takes the slot before it stamps the row, so `max-in-flight` is also the
  platform-thread bound. The library's bytecode stays on Java 17.
- `OutboxPublisher.deliver(event)` (returns a result for the booking; the
  default calls `publish`), `OutboxBooking` (outcome, result, failure,
  exhaustion).
- `opentmf.outbox.shutdown-grace` (default 10 s): on stop, sends in flight get
  this long to finish and book; the rest lapse by lease.
- Gauge `opentmf.outbox.in-flight` (pending rows under a live lease, across
  pods); `OutboxRowView` gains `inFlight` and `claimedUntil`. The `pending`
  gauge still counts in-flight rows.
- Changeset `004-outbox-claim-lease`: `claimed_until` plus the partial index
  `ix_outbox_claimed_until`, additive and `if not exists`, so it applies to an
  onboarded pre-library table too. 001–003 are untouched (their checksums
  hold, pinned against the released 1.2.1 changelog).

### Internal

- The claim scan reads in-flight rows too (to learn the ordering keys taken)
  and locks with a waiting `FOR UPDATE` instead of `SKIP LOCKED`. Every lock
  holder is a short transaction now, and waiting lets a claim see a row
  another pod is stamping as in flight instead of skipping past it to a later
  row of the same key. The scan reads at most ten windows of `batch-size`
  rows.
- The relay's nudges coalesce, and a freed CONCURRENT slot nudges the relay.
- The ITs run twice over the lane: on JDK 17 (platform threads) and, for
  `OutboxHttpLaneVirtualIT`, in a failsafe execution forked on a JDK 21+
  toolchain (virtual threads).

## 1.2.1 - 2026-09-21

### Fixed

- **A consumer without `spring-kafka` could not start.** `OutboxAutoConfiguration`
  named `KafkaTemplate` in a bean-method signature; the method-level
  `@ConditionalOnClass` did skip the bean, but Spring still introspects the
  auto-configuration class reflectively to resolve its other factory methods,
  and the missing type threw `NoClassDefFoundError: org/springframework/kafka/core/KafkaTemplate`
  before any context came up. The Kafka publisher now lives in a nested,
  name-guarded member class (`KafkaPublisherConfiguration`), so a Kafka-less
  consumer starts with the HTTP publisher only and nothing Kafka-typed is
  linked. Found on dnms-catalog 1.2.0 (Yusuf, 2026-09-11) — the first consumer
  without Kafka; the six Kafka consumers were never affected. A consumer that
  worked around it by adding `spring-kafka` and excluding
  `KafkaAutoConfiguration` can drop both at its next touch.

### Changed

- **The HTTP publisher's guard is name-based and nested** for the same reason
  (`HttpPublisherConfiguration`, `@ConditionalOnClass(name =
  "org.springframework.web.client.RestClient")`): a web-less consumer (a pure
  Kafka relay) also starts. No behaviour change for any consumer that has
  spring-web.
- Neither nested class is `@Configuration`: a stereotype would make it a
  component-scan candidate, and a consumer whose scan root covers
  `org.opentmf.outbox` would register it ahead of the auto-configuration
  order, where `@ConditionalOnBean(KafkaTemplate)` evaluates before
  `KafkaAutoConfiguration` exists and the publisher silently vanishes. Lite
  member classes are processed only through the outer auto-configuration.
- Compiled against `tmf630-toolkit-all` 3.3.0 (was 3.1.1) — the line the
  opentmf-versions BOM pins for consumers; the dependency stays optional.
- `KafkaLessStartupTests` pins all of it: a child-first test classloader that
  DEFINES the library classes without the hidden package (Boot's
  `FilteredClassLoader` cannot reproduce the reflective failure), a signature
  scan of the outer class, and the no-stereotype rule on the nested ones.

## 1.2.0 - 2026-08-27

The last gap-closing release: the union of the consumer audits (dnms-flow,
dnms-681, the email and inbox adapters) and the library's own, plus the
release gate that turns a future consumer gap into a red library build.
Additive throughout — a 1.0.0 or 1.1.0 consumer upgrades unchanged; the
included changelog applies `003-outbox-policy-reference-onboarding` on the
next start.

### Added

- **Per-publisher failure policy.** `OutboxPublisher` gains three default
  methods — `maxAttempts(event)` (0 = library `max-attempts`),
  `backoff(event, attempt)` (null = library backoff) and `onExhausted(event)`
  → `PARK` (default) or `DROP`. The worker resolves the publisher first and
  books every failure with its policy. DROP stamps `relayed_on`, keeps
  `last_error`, increments `opentmf.outbox.dropped{destination}` and logs a
  WARN; no relayed listener fires and `relayed` is not incremented. A publisher
  throws `TerminalOutboxException` to reach the exhaustion outcome immediately.
- **Explicit park stamp.** `parked_on timestamptz` (nullable): stamped at
  exhaustion-PARK, cleared by `unpark` (which also resets attempts). The claim
  predicate reads the stamp instead of an attempt count (`parked_on is null`
  replaces `attempts < max-attempts`), so per-publisher budgets are honoured
  by the claim itself; the `parked` gauge, `OutboxStateFilter.PARKED` and
  `OutboxRowView.parked` read the column. A row parked under 1.1.0 (no stamp)
  becomes claimable again and parks with the stamp on its next failure.
- **Private per-row `reference`** (`varchar(128)`, frozen at write):
  `OutboxAppend.withReference`, `OutboxRowView.reference`, filterable on the
  ops list — never forwarded to the wire by either built-in publisher. The
  rule: `headers` = wire, `reference` = private.
- **`OutboxHeaders`** — the three relay header names and
  `idempotencyKey(serviceName, id)` are public: the key format is a
  cross-service contract.
- **Onboarding of a pre-library `outbox` table, in the library.** `001` is
  `onFail:MARK_RAN` when the table exists, `002` when either of its columns
  exists, and `003` adds every library column that is missing
  (`add column if not exists`) and recreates `ix_outbox_pending` to the 1.2.0
  predicate. The guards live outside the SQL bodies, so already-recorded
  checksums are unchanged (`OutboxOnboardingIT` upgrades a database migrated
  by the released 1.1.0 changelog). Consumer-specific deltas — a `NOT NULL` /
  `DEFAULT` on `release_at`, columns the library does not know — stay the
  consumer's; the README section "Adopting with an existing `outbox` table"
  replaces the former "no onboarding shims".
- **Ops wire contract.** The library controller maps `IllegalArgumentException`
  → 404 and `IllegalStateException` → 409 (`ResponseStatusException`, no
  advice bean). `GET /ops/outbox/state/{state}` narrows the list to one
  derived-state leg (`pending|parked|relayed|cancelled`, unknown → 400) with
  the TMF630 attribute filters and paging still applying on top;
  `/ops/outbox/parked` stays as its alias. The state rides the path because
  tmf630-toolkit 3.1.1 rejects any non-reserved query parameter before the
  handler runs (a `?state=` form is a toolkit backlog item).
- **The release gate**: per-publisher policy tests (incl. drop-fires-no-
  listener), reference-absent-from-both-wires, header-collision-replaces on
  both legs, crash-window redelivery and SKIP LOCKED contention ITs, the
  onboarding IT (fresh / 1.0.0-shaped / 681-shaped / upgrade-from-1.1.0), and
  three consumer-conformance ITs kept in the library — `Profile681HubIT`,
  `ProfileFlowHttpSideEffectIT`, `ProfileAdapterKafkaOrderIT`.

### Consumer action

- **Global exception handlers must honour Spring `ErrorResponse`.** The
  library controller now answers 404/409/400 through
  `ResponseStatusException` (the Spring contract, pinned by
  `OutboxOpsControllerTests` independent of any mapper). A service whose
  global exception handler swallows `ErrorResponse` — the dnms template
  `GlobalExceptionMapper` before its fix — turns them into a generic 500, and
  for an unknown row id that is a regression from the 400 such a service
  answered on 1.1.0. Adopt the template fix in the same release as this
  upgrade.
- **Test fixtures that seed parked rows must set `parkedOn`.** Attempts alone
  no longer park a row (B): a row seeded with `attempts = max-attempts` and no
  `parked_on` is pending and claimable, and `OutboxRowView.parked` is false
  for it.

### Fixed

- **HTTP header collision.** The HTTP publisher appended a stored header that
  collided with a relay header (`x-event-type` went out twice); it now replaces
  it, as the Kafka leg always did.

### Changed

- Contracts now stated as supported (Javadoc + README): a publisher may write
  to the database inside the claim transaction; listeners run in bean order
  after the stamp; per-pod id order vs cross-pod SKIP LOCKED interleave; the
  Kafka value is the stored JSON string (string-compatible serializer);
  `traceparent` is the consumer's `spring.kafka.template.observation-enabled`;
  held rows count as pending and not as relay-lag; cancel guards on relayed;
  `cancel`/`unpark` block (no lock timeout) for one in-flight publish.
- README corrections from the audit: `backoff-factor` is a double;
  `ops-endpoints` is a conditional key, not a bound field; the
  auto-configuration is unconditional (no `DataSource` guard); HTTP
  destinations need `spring-web`; retention prunes relayed AND cancelled; the
  public-contract list names `OutboxAppend`, `OutboxHeaders`,
  `OutboxRelayedListener`, `TerminalOutboxException`; the `unknown`
  service-name fallback; `EntityManager` as the only seal-safe test seeding;
  the unverifiable PIT figure removed.
- `OutboxRowView` gains `reference` and `parkedOn` (record components — the
  canonical constructor changes; `of` was never public).
- `OutboxAppend` gains `reference` (record component — the canonical
  constructor changes; `of` + withers are the API).

## 1.1.0 - 2026-08-27

Additive — a 1.0.0 consumer upgrades with zero changes; the included changelog
applies changeset `002-outbox-hold-and-cancel` (two nullable columns) on the
next start.

### Added

- **Scheduled sends.** A row can carry a hold: `release_at` (nullable), set
  through the new request-shaped `OutboxWriter.append(OutboxAppend)`
  (`OutboxAppend.of(...).withReleaseAt(...)`; the positional overloads keep
  their meaning — no hold). A held row is not claimable before that instant and relays normally
  afterwards. The hold is frozen at write time — a delivery failure's backoff
  reschedules `next_attempt_on` only and can never move `release_at` (the
  mapping is `updatable = false`; regression test
  `backoff_neverTouchesTheReleaseHold`).
- **Cancellation of an unreleased effect.** `cancelled_on` (nullable), set by
  the guarded `OutboxMaintenanceService.cancel(id)` and
  `POST /ops/outbox/{id}/cancel`: only a row that is neither relayed nor already
  cancelled is cancellable — a relayed row refuses with an
  `IllegalStateException` ("already relayed"). Cancelled rows are never
  relayed, are retained for audit, and are pruned on the same retention as
  relayed rows. The state model gains its fourth derived leg, `cancelled`
  (`OutboxStateFilter.CANCELLED`; `pending` and `parked` now exclude cancelled
  rows); `OutboxRowView` carries `releaseAt` and `cancelledOn`.
- The ops actions (`cancel`, `unpark`) read their row under a waiting
  `FOR UPDATE`, so an action racing a relay claim in flight sees the row as the
  relay left it — a cancel never silently marks a delivered effect cancelled.
- `OutboxMaintenanceService.prune()` — prunes relayed and cancelled rows;
  `pruneRelayed()` stays as its alias.

### Changed

- The `pending` and `parked` gauges exclude cancelled rows; `relay-lag` now
  measures how long the oldest *released* pending row has been deliverable — a
  held row does not register as lag until its hold passes.

## 1.0.0 - 2026-08-26

Initial release.
