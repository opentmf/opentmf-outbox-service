# Changelog

## 1.3.0 - 2026-10-06

OUTBOX-HTTP-LANE-1: HTTP rows leave the ordered relay, and every row is claimed
by **lease**. Until now the relay was one thread that published up to
`batch-size` rows one after another inside ONE transaction. A slow HTTP
subscriber delayed every Kafka row behind it, and a database connection plus
the batch's row locks were held across every backend call.

### Fixed

- **The metrics gauges read the whole table on every scrape.** `pending`, `parked` and
  `relay-lag` ran unindexed queries over every row, relayed history included. On
  3.5–4.8 M-row tables that took 7–20 s per query and about 3 PostgreSQL cores with no
  traffic. Scrapes outlasted their 10 s timeout, so flow and 681 read `up=0` exactly
  under load (load test 2026-10-06, F-2).
  - The gauges now read a snapshot that a daemon thread refreshes every
    `opentmf.outbox.metrics-refresh` (default 15 s), so a scrape never touches the
    database. Before the first refresh, and after a failed one, they read NaN.
  - Each refresh query has a 5 s query timeout, so a database that does not answer
    becomes a failed refresh, not a hung refresher with stale values standing. A new
    gauge `opentmf.outbox.metrics-age` gives the seconds since the last successful
    refresh; alert on it beside `parked`.
  - Each refresh query is served by a partial index over the OPEN rows only
    (`ix_outbox_pending`, `ix_outbox_parked`, `ix_outbox_open_since`,
    `ix_outbox_claimed_until`). Pinned in `OutboxClaimPlanIT` over 1,000,000 relayed
    rows, under both plan-cache modes.
- **The retention prune could not prune a large table.** `prune()` ran one transaction
  around Spring Data derived deletes. It loaded every expired row as an entity, found
  them by a full scan (`relayed_on` had no index), and removed them one by one. The
  nightly job ran dnms-681 and dnms-flow out of CPU and memory, failed liveness twice
  each, and pruned nothing (load test 2026-10-06, F-1).
  - The prune now deletes set-based in batches (`opentmf.outbox.maintenance.batch-size`,
    default 5,000), oldest first. Each batch is its own short transaction
    (`REQUIRES_NEW`, so a caller already in a transaction neither holds the batches in
    it nor undoes them with its rollback), driven by
    `ix_outbox_relayed_on` / `ix_outbox_cancelled_on`, with no entity loaded.
  - A call is bounded by `opentmf.outbox.maintenance.time-budget` (default 10 s). The
    endpoint answers `{"outboxRowsPruned": n, "moreToPrune": bool}`: the 1.0.0 key is
    unchanged, `moreToPrune` is new, and the caller calls again while it is true.
  - `OutboxMaintenanceService.pruneExpired()` returns an `OutboxPruneResult`.
    `prune()` / `pruneRelayed()` still return the count but are no longer one
    transaction, and a call may leave expired rows for the next one.
- Changeset `005-outbox-gauge-and-prune-indexes` adds the four indexes. They are built
  `CONCURRENTLY` (the changeset runs outside a transaction), so a deploy onto a large
  table does not block the appends of the pods still running. Each build is preceded by
  `drop index concurrently if exists`, so a failed build's INVALID index is rebuilt,
  not skipped. `ix_outbox_relayed_on` is about 107 MB on 5,000,000 relayed rows.

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
- **The lane and ordering key are stamped at APPEND**, frozen like the
  headers: the writer asks the row's publisher through the relay's router.
  The stamped lane wins at claim time, so changing a publisher's lane affects
  only rows appended afterwards. A row not written through `OutboxWriter`
  (raw SQL, migrated data, rows from before 1.3.0) has no lane and rides
  ORDERED.
- While a 1.3.0 relay holds a row's lease, the row's `next_attempt_on` reads
  as the lease end (the claim moves it there; every booking sets it again).
  This is what keeps a 1.2.x pod off leased rows during a rolling upgrade.
  While old and new pods overlap, sends are not duplicated, but per-key order
  is not guaranteed.

### Added

- **The `/ops/outbox` OAS fragment, shipped in the jar** (PR #4).
  `META-INF/openapi/opentmf-outbox-ops.oas.yaml`, read through
  `OutboxOpsOpenApi.fragment()`, documents the eight ops routes ONCE.
  - It includes 1.3.0's unpark by filter, `moreToPrune` / `moreToUnpark`, and
    `inFlight` / `claimedUntil` on the row, and names the meters (`metrics-age`,
    `in-flight` among them).
  - It documents the TMF630 paging the toolkit actually answers: 200 / 206 / 416,
    `X-Total-Count`, `X-Result-Count`, a 1-based `Content-Range`, `Link`, `limit`
    clamped at `max-limit` rather than rejected, and the toolkit's own error object
    on 400 and 416.
  - Four consumers were describing this surface four different ways, none with the
    paging. Each now pastes the fragment and may pin itself with a drift test
    (README "OAS fragment").
- **Guards for the fragment.** `OutboxOpsOpenApiTests` fails when the fragment and
  `OutboxOpsController` disagree on a route, or when the `OutboxRow` schema and
  `OutboxRowView` disagree on a field. `OutboxRoundTripIT` pins the paging contract
  against the real tmf630-toolkit (partial page, past-the-end, clamp, empty result),
  so a toolkit bump that changes it fails here first.
- **Unpark by filter** (OUTBOX-BULK-UNPARK-1). `POST /ops/outbox/unpark` (JSON body)
  and `OutboxMaintenanceService.unpark(destination, parkedFrom, parkedTo, reference)`
  return every parked row of ONE destination to delivery. The destination is required;
  a `parked_on` range and a `reference` are optional.
  - Each row ends as the single unpark leaves it, under the same waiting-lock guard.
  - Set-based and bounded like the prune (`opentmf.outbox.maintenance.batch-size` /
    `time-budget`). The answer is `{"outboxRowsUnparked": n, "moreToUnpark": bool}`
    (`OutboxUnparkResult`).
  - The resend stays bounded by the lanes. A parked row is never leased, so no
    unparked row carries a lease.
  - Served by `ix_outbox_parked (destination, parked_on, id)` in changeset 005.
- **Two lanes, chosen by the publisher:** `OutboxPublisher.lane(event)` is
  `ORDERED` (the default: the single relay thread, `id` order) or `CONCURRENT`
  (parallel sends, at most `opentmf.outbox.concurrent.max-in-flight`, default
  8, per pod). The Kafka publisher stays ORDERED and the HTTP publisher is
  CONCURRENT.
- **Ordering key** for the CONCURRENT lane: `OutboxPublisher.orderingKey(event)`.
  Rows sharing a non-null key are never in flight together, within a pod and
  across pods, and are taken in `id` order on the happy path. Across pods, a
  keyed candidate is claimed only after its key's transaction-scoped PostgreSQL
  advisory lock is taken (`pg_try_advisory_xact_lock`, never waited for) and a
  re-check in a fresh snapshot confirms it; the claim transactions run READ
  COMMITTED. A key over 255 characters is stored as `sha256:<hex>`.
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
- Changeset `004-outbox-claim-lease`: `claimed_until`, `lane` and
  `ordering_key`, plus the partial indexes `ix_outbox_claimed_until`,
  `ix_outbox_ordered_claim`, `ix_outbox_concurrent_keyed` and
  `ix_outbox_concurrent_unkeyed`. It is additive
  and `if not exists`, so it applies to an onboarded pre-library table too.
  001–003 are untouched (their checksums hold, pinned against the released
  1.2.1 changelog). Its four indexes are built `CONCURRENTLY` (the changeset
  runs outside a transaction), each preceded by `drop index concurrently if
  exists`, so a deploy does not block the appends of the pods still running and
  a failed build's INVALID index is rebuilt, not skipped. With 005's four, the
  eight builds take about 2.3 s per million rows (7.2 s on 3.1 M rows / 2.5 GB);
  the consumer's startup probe must allow for the first start.

### Dependencies

- tmf630-toolkit 3.3.0 → 3.4.0; archunit 1.5.0 → 1.5.1. Parent
  `spring-boot-starter-parent` 4.1.1 is the latest release.
- **Named hold:** pitest-maven stays at 1.19.6 (1.30.0 available). Free
  incremental history (`withHistory`) moved behind the commercial arcmutate
  plugin from 1.20, and the bare flag fails the run there. Decided by Gökhan;
  the standing hold of this repo.

### Internal

- Each lane claims over its own query and partial indexes with
  `FOR UPDATE SKIP LOCKED`, each in its own short transaction on the relay
  thread (the CONCURRENT one only when a lane slot is free). Neither reads
  relayed rows or the other lane's backlog.
- The CONCURRENT claim is native SQL (`OutboxClaimSql`).
  - It reads the in-flight keys through the lease index (a live lease holds
    its key even on a cancelled row).
  - It steps key by key through `(ordering_key, id)`, round-robin from a
    per-pod cursor, with one head probe per key not in flight, and stops at
    the free slots.
  - Its cost is bounded by those slots plus the keys stepped over. It does not
    grow with the table or with one key's backlog: 0.5–1.3 ms with 5,000,000
    relayed rows, including a 100,000-row one-key backlog and 10,000 keys.
  - Two statement texts: the first pass with a plain lower key bound, and a
    wrap-around pass with a plain upper bound. No parameter-dependent `OR`.
    The in-flight keys are an array tested with `<> all(...)`, not a join.
    So the shape holds under a custom and a generic plan alike; the library
    never sets `plan_cache_mode`.
  - The locked row is re-checked against the WHOLE eligibility predicate
    (hold and due legs included). A row another pod booked into backoff
    since the snapshot is not taken again.
  - `ordering_key` is `collate "C"`, so the database's key order and the
    relay's cursor order agree.
  - `OutboxClaimPlanIT` pins the plan shape, filter-free key scans and the
    buffer count under both plan modes.
  - What still grows with the data: rows that are pending but not yet due
    (held or in backoff) on the claim's lane, about 17–18 ms per 100,000.
    The README states it.
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
