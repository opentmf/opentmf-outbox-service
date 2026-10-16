# Changelog

## 1.8.0 - 2026-10-16

A slow ORDERED receiver no longer stalls the CONCURRENT lane, the unpark by filter no longer
coerces a wrongly typed field, and a consumer whose error model is not RFC 9457 can paste the ops
fragment's paths verbatim.

### Fixed

- **The CONCURRENT lane no longer waits behind an ORDERED batch**
  (OUTBOX-CONCURRENT-CLAIM-INDEPENDENT-1). Since 1.3.0 the relay thread claimed both lanes and
  then sent the ORDERED batch itself, row after row. The CONCURRENT rows already handed over
  went on sending, but the lane's next claim waited until the last ORDERED row of the batch was
  booked. With a slow ORDERED receiver that is a long stall: in an end-to-end run, the
  CONCURRENT lane relayed nothing for 26 s while ORDERED HTTP legs went out at about one a
  second, then sent over 200 rows in two seconds.
  - The ORDERED batch now goes to the **ORDERED lane's own thread** (`opentmf-outbox-ordered`),
    and the relay thread only claims. A CONCURRENT row appended while an ORDERED batch sends is
    claimed at once.
  - The ORDERED lane keeps its guarantees: one batch in flight per pod (the relay claims it
    only while the lane is idle, and the lane nudges the relay when a batch is done), rows one
    at a time in `id` order, each under its short lease renewed by the booking of the row
    before it.
  - Measured by `OutboxLaneIndependenceIT`: 100 ORDERED rows to a receiver answering in 150 ms
    (a 15 s batch) are sending when a CONCURRENT row is appended. It is delivered within two
    seconds, while the ORDERED batch is still going. On 1.7.0 it waited for the batch.
- **The unpark by filter types its body strictly** (OUTBOX-UNPARK-FILTER-STRICT-TYPES-1). It
  bound `destination` as a plain string through the consumer's mapper, and a default mapper
  coerces scalars: `{"destination": false}` became the destination "false", passed the blank
  check and answered 200 (a black-box suite's negative test found it). The library now reads
  the JSON object itself, whatever the consumer's mapper does, and never coerces: `destination`
  and `reference` take a string or null, `parkedFrom` / `parkedTo` an RFC 3339 date-time with
  an offset or null. A boolean, a number, an object or an array - and a date-time without an
  offset, or a number read as epoch seconds - is a 400 naming the field; nothing is unparked.
  A sent offset is now kept as sent rather than normalised to UTC (the instant is the same).
  `OutboxOpsUnparkFilterStrictTypesTests` sends `false`, `true`, `0`, `1`, `1.5`, `{}` and
  arrays on every field; `OutboxOpsProblemDetailsIT` shows the 400 through the application's
  own mapper (it was a 200).
- **Pool sizing:** the relay thread's claim and an ORDERED booking can now hold a connection at
  the same moment. Size the pool above `concurrent.max-in-flight` + 3 (was + 2).
- **Shutdown:** the relay thread, then the ORDERED batch, then the CONCURRENT sends each get
  `shutdown-grace` in turn; what is not booked by then is redelivered once its lease lapses, as
  before.

### Changed (the OAS fragment - re-paste it)

- **Every error is a refinable component; the paths are verbatim for every error model**
  (OUTBOX-FRAGMENT-ERROR-MEDIA-TYPE-REFINABLE-1). The 1.7.0 fragment documented the library's
  own refusals - which the consumer renders - as `application/problem+json`, in four inline
  path responses and three unmarked components. A consumer whose single advice answers every
  error as `application/json` in its own error shape could not paste it without promising a
  wrong media type. Now:
  - the four inline responses are components: `outboxInspectBadRequest` (the inspect GET's
    400), `outboxUnparkConflict` and `outboxCancelConflict` (the 409s), and
    `outboxUnparkFilterBadRequest` (the bulk unpark's 400). Every error in the paths is a
    `$ref`, so the paths are pasted verbatim, whatever the consumer's error model;
  - the library's refusals are marked **`x-opentmf-refinable: content`**: `outboxBadRequest`,
    `outboxRangeNotSatisfiable`, `outboxRowIdOutOfRange`, `outboxRowNotFound` and the four
    new ones. Their `description` (when the answer happens) and `headers` are the library's and
    stay verbatim; their `content` is the consumer's rendering, media types and schemas alike.
    What ships is the default (Spring's RFC 9457 rendering and the toolkit's `Tmf630Error`),
    and no such description names a media type any more;
  - `x-opentmf-refinable: true` is unchanged: exactly the six consumer answers and
    `OutboxProblem`, redefinable whole.
  - A drift test compares the paths exactly, `content` components by description and headers,
    `true` components by name, and everything else exactly (README "OAS fragment").
    `OutboxOpsFragmentDriftRuleTests` is that rule as code: a verbatim paste and a consumer
    that answers every error as its own JSON both pass with the paths untouched.
- **Consumers:** re-paste the fragment (the paths changed: four inline error responses are
  `$ref`s now). A drift test written for 1.7.0 keeps passing after the re-paste for a consumer
  that refines only the `true` components; a consumer that refines a refusal's content adds
  the `content` rule.

## 1.7.0 - 2026-10-15

The ops surface's contract, as the first black-box suites run against a pasted fragment found
it: the shipped OAS fragment says what the routes really do, a verbatim paste of it is
complete, and a literal sibling segment is never read as a row id.

### Changed (behaviour - read before upgrading)

- **The row routes take an id of digits only** (OUTBOX-OPS-ID-PATH-DIGITS-1). `GET
  /ops/outbox/{id}` and the `{id}/unpark` and `{id}/cancel` POSTs were mapped without a
  constraint, so `GET /ops/outbox/unpark` reached the inspect route and answered 400 ("not a
  whole number"), and `OPTIONS /ops/outbox/unpark` advertised GET. They are `{id:\d+}` now:
  - `GET /ops/outbox/unpark` is a 405 (the path serves POST only), and `OPTIONS` on it names
    POST alone;
  - any other segment that is not digits (`/ops/outbox/state`, `/ops/outbox/-1`,
    `POST /ops/outbox/abc/cancel`) matches no route: a 404 where it was a 400;
  - digits beyond a `long` are still Spring's binding refusal, a 400.

  Every valid request answers as before. Only a check that expected the 400 on a wrong path
  sees a different code.

### Changed (the OAS fragment - re-paste it)

`META-INF/openapi/opentmf-outbox-ops.oas.yaml`, `info.version` 1.7.0. The paths and the
2xx shapes are unchanged; the fragment loses one bound and gains the error answers.

- **`limit` declares no `maximum`** (OUTBOX-OAS-LIMIT-CLAMP-1). The parameter said
  `maximum: 500` while its description, and the toolkit, CLAMP a larger value to
  `paging.max-limit` (`limit=501` is a 200). A black-box suite read the bound as "501 must be
  refused". The clamp stays; the schema now agrees with it.
- **Every error is documented, with its body** (OUTBOX-OAS-FRAGMENT-SECURITY-RESPONSES-1).
  Before, the fragment named only the library's own 4xx, some without a body, and every
  consumer layered its security and transport answers by hand, which broke exact-equality
  drift tests. Now:
  - every operation names, by `$ref`, the answers the consumer gives before it runs: 401
    `outboxUnauthorized`, 403 `outboxForbidden`, 405 `outboxMethodNotAllowed`, 406
    `outboxNotAcceptable`, 503 `outboxServiceUnavailable`, and 415
    `outboxUnsupportedMediaType` on the bulk unpark's JSON body;
  - every error the consumer renders carries `OutboxProblem`, a generic RFC 9457 problem
    (`type`, `title`, `status` as a number, `detail`, `instance`; other members allowed), as
    `application/problem+json` - what Spring writes with problem details on. The library's
    own refusals gain it too: `outboxRowNotFound` (404) and `outboxRowIdOutOfRange` (400) on
    the three row routes, the 409s, the bulk unpark's 400;
  - `outboxBadRequest` on the three lists documents both renderings: the toolkit's
    `Tmf630Error` (`application/json`) and the unknown state leg's problem;
  - `outboxRowId` says digits only (`minimum: 0`);
  - tmf630-toolkit's undeclared-query-parameter guard (3.5.0, opt-in, `IGNORE` by default):
    where a consumer switches it to `REJECT`, its 400s are documented too - Spring's
    `page` / `size` on the three lists (`outboxBadRequest`, `Tmf630Error`) and a query
    parameter the inspect GET does not take (its 400 now lists `Tmf630Error` beside
    `OutboxProblem`).

  `OutboxOpsProblemDetailsIT` measures the media types on the real app, with the real toolkit,
  `spring.mvc.problemdetails.enabled=true` and the guard on `REJECT`: the toolkit's own 400
  and 416 stay `application/json` beside Boot's problem handler; the state leg's 400 and the
  406 are `application/problem+json`.
- **Refinable by name.** The six consumer answers and `OutboxProblem` carry
  `x-opentmf-refinable: true`. A consumer may redefine a marked component under the SAME name
  (its wording, headers such as `Retry-After`, its own problem schema - e.g.
  `OutboxProblem: { $ref: '#/components/schemas/ProblemDetail' }`) and paste everything else
  verbatim. A drift test compares the paths exactly and the components exactly unless
  marked (README "OAS fragment" shows one).
- **Consumers:** re-paste the fragment. Hand-layered 401/403/405/406/415/503 on the ops
  routes are now part of it, and a named waiver for the `limit` bound or for
  `/ops/outbox/unpark` can go.

### Dependencies

- tmf630-toolkit 3.4.0 → 3.5.1: the opt-in undeclared-query-parameter guard (default
  `IGNORE`: nothing changes until a consumer switches it on; the fragment documents its
  `REJECT` answers, above).
- central-publishing-maven-plugin 0.11.0 → 0.12.0 (release profile only).
- Held by name: pitest-maven 1.19.6 (latest 1.30.1), Gökhan's hold. Every other
  property and plugin is current; the parent spring-boot-starter-parent 4.1.1 is the latest
  GA.

### Added (tests)

- `OpsFragmentConformance`: a black-box check in miniature. A request answers the expected
  status, the fragment documents it on that operation, and names the media type it was
  written in. `OutboxOpsWireConformanceTests` (standalone, Spring's RFC 9457 advice) and
  `OutboxOpsProblemDetailsIT` (the real application) use it.
- `OutboxOpsRoutingTests` and `OutboxRoundTripIT` pin the digits-only row routes;
  `OutboxOpsOpenApiTests` pins the per-operation codes, a body on every error, the refinable
  set and that every `$ref` resolves.

## 1.6.0 - 2026-10-13

The relay's claim cadence: one pod moves an order of magnitude more CONCURRENT rows, and
`concurrent.max-in-flight` finally governs the lane's throughput.

### Changed (behaviour - read before upgrading)

- **The CONCURRENT claim runs ahead of the slots** (OUTBOX-RELAY-CLAIM-CADENCE-1). On the
  load box (dnms-flow at 25 req/s, about 400 outbox rows/s produced), one pod relayed about
  110 rows/s whatever `concurrent.max-in-flight` said: 8 and 64 slots gave the same rate.
  Kafka answered in 1.5 ms, every statement took under 10 ms, and GC was idle. With intake
  off the same relay drained at 250 rows/s. The relay thread was the limit. For every row
  it claimed it paid two statements: a `pg_try_advisory_xact_lock` call per key and a
  flushed update per stamped row. It also claimed only as many rows as there were free
  slots. Throughput was therefore about 1 / (2 × statement time).
  - The relay now claims CONCURRENT rows into the lane's **hand-off queue**. Whenever the
    queue is at most half full, it tops it back up to `batch-size` rows. `max-in-flight`
    workers send the rows in claim order, so the slots bound the sends, not the claim.
  - **One statement per claim**, whatever the row count:
    - one tries every candidate key's advisory lock (`OutboxClaimSql.KEY_LOCKS`);
    - one re-checks the locked candidates, as before;
    - one stamps every lease of the same length (`OutboxClaimSql.STAMP`).

    The lock statement unnests an array and reads no table. It takes the same lock ids
    as before (`hashtext` of the key, same namespace), so 1.5.x and 1.6.0 pods contend
    for the same locks during a rolling deploy.
  - A **RELAYED booking is one guarded statement** (`OutboxClaimSql.BOOK_RELAYED`, an
    `update … returning` in a data-modifying `with`). Before, it was a locking read plus a
    flushed update. The booked row still comes back managed, for the booking hook and the
    relayed listeners, which now also see `relayed_on` in the database inside the booking
    transaction.
  - Measured by `OutboxRelayThroughputIT`: 10,000 pending CONCURRENT rows over 5,000
    keys, a 1 ms publisher, ONE relay, and every statement answered 5 ms late (a Toxiproxy
    latency toxic on the database link). Every run also checks that no key had two sends
    overlapping and that each key's rows went in `id` order.

    | `max-in-flight` | 1.5.0 | 1.6.0 |
    |---|---|---|
    | 8 (the default) | 49 rows/s (202.8 s) | 562 rows/s (17.8 s) |
    | 64 | 79 rows/s (127.3 s) | 1,520 rows/s (6.6 s) |
- **A queued row's lease.** A row waiting in the hand-off queue holds its lease. One that
  waited **a tenth of its lease or longer is renewed right before its send**, guarded on
  its stamp like the ORDERED lane's renewal:
  - a row another relay took meanwhile is not sent;
  - a cancelled row is released unsent.

  Every CONCURRENT send therefore starts with at least nine tenths of its lease. On the
  fast path, where the queue drains in milliseconds, nothing extra happens.
- **The ORDERED claim runs only when ORDERED rows may be due.** That means: after a row of
  that lane (or of no known lane) was appended or unparked in this pod, after an ORDERED
  claim that came back full, on the first pass, and on every sweep. A CONCURRENT-only
  deployment pays one claim per pass instead of two. The sweep picks up what no nudge
  announces: an ORDERED retry come due, a lapsed ORDERED lease, a row another pod appended
  and could not relay. Under load these are now picked up within `sweep-interval`
  (default 5 s) rather than on the next pass. Nudges coalesce as before: any number during
  a pass make ONE follow-up pass, and `OutboxRelayTests` now pins that.
- **Isolation: the CONCURRENT lane's bookings and renewals run at the application's
  default isolation.** The claims and the ORDERED bookings keep the READ COMMITTED pin
  (coordinator ruling, 2026-10-13):
  - The pin exists for the claim's re-check, which must see, in a new snapshot, leases
    another pod committed.
  - An ORDERED booking renews the next row in the same transaction, so a serialization
    error there would roll back a delivered row's booking.
  - The pin cost a `SHOW TRANSACTION ISOLATION LEVEL` round trip per transaction: the
    driver reads the current level from the server before setting it. That was a third
    of a CONCURRENT row's booking.

  The default is READ COMMITTED on PostgreSQL. A consumer that runs another default
  pays a serialization-error path on a lease race (a relay re-taking the row after its
  lease lapsed), never a wrong booking: the booking falls through to the failure path,
  which finds the lease gone and books nothing. `OutboxLaneBookingStatementsIT` reads
  PostgreSQL's statement log and pins both sides:
  - a CONCURRENT booking is `BEGIN`, one `update … returning`, `COMMIT`;
  - the claim still opens with the `SHOW`.
- **The `in-flight` gauge reads higher under load.** `opentmf.outbox.in-flight` (lane
  `concurrent`) now also counts the CONCURRENT rows waiting in a hand-off queue, since they
  hold a lease: up to `batch-size` more per pod than on 1.5.x. Re-check dashboards and alerts
  that read `opentmf.outbox.in-flight` - a threshold tuned to `max-in-flight` will now fire
  under ordinary load.
- **Shutdown** drops the rows still queued unsent. Like any send that outlives the
  shutdown grace, they are redelivered once their leases lapse.

### Added

- `OutboxRelayThroughputIT` (the figures above), `OutboxLaneBookingStatementsIT`, and pins
  in `OutboxClaimPlanIT` for:
  - a full 100-row claim: 7 ms, 1,314 buffers under both plan modes;
  - the one-statement key locks: 0.5 ms, no table read;
  - the stamp: 2–3 ms;
  - the RELAYED booking: 0.4 ms, 15 buffers.
- Test-scoped `org.testcontainers:testcontainers-toxiproxy` (BOM-managed).

**Compatibility.**
- No schema change, no new changeset.
- No property added or renamed:
  - `batch-size` now also sizes the CONCURRENT hand-off queue;
  - `concurrent.max-in-flight` is now the lane's throughput lever. Each sending slot holds
    a connection while it books, so size the connection pool above `max-in-flight` + 2.
- Mixed 1.5.x / 1.6.0 pods share the lock ids and the lease columns, so per-key order
  holds across them during a rolling deploy.

## 1.5.0 - 2026-10-09

Per-aggregate Kafka order across several relays.

### Added

- **`opentmf.outbox.kafka.lane: ORDERED | CONCURRENT`** (OUTBOX-KAFKA-PER-KEY-ORDER-1).
  The library's default Kafka publisher was fixed to the ORDERED lane. With several pods
  that lane interleaves: two pods never take one row twice, but one aggregate's rows could
  reach Kafka out of order. An adapter's `delivered` could then land ahead of its
  `accepted`, and a consumer reading "no accept, so not sent" recorded a sent message as
  not sent.
  - On `CONCURRENT`, a Kafka row is keyed by `opentmf.outbox.kafka.ordering-key`
    (`AGGREGATE_ID`, the default). One aggregate's rows are never in flight together on
    any pod and go in `id` order on the happy path, while different aggregates relay in
    parallel. `NONE` drops the order (every row independent).
  - The record key stays the raw `aggregateId`, as before, so partitioning does not move
    and partition order matches. A key over 255 characters is still sent as is; only its
    stored ordering key is hashed.
  - `opentmf.outbox.kafka.lease` (default 15 s, validated to exceed `send-timeout`) is the
    lease of a Kafka row on `CONCURRENT`. It is short, like the ORDERED lease, so a pod
    stop delays redelivery by seconds.
  - The header contract (`x-idempotency-key`, `x-event-type`, `x-producer`, `traceparent`)
    is unchanged.
  - `OutboxKafkaTwoPodsIT` proves it on two Spring contexts (two relays, two producers)
    over one PostgreSQL and one broker: 40 aggregates × 3 lifecycle rows, appended
    alternately from both pods and released at one instant, with every send slowed at
    random. Every aggregate's rows arrive in order and every row exactly once. On
    `ORDERED` (`-Dit.kafka.lane=ORDERED`) the same IT fails.

**Compatibility.** Nothing changes until a consumer sets `kafka.lane: CONCURRENT`; the
default stays `ORDERED`.
- The lane is stamped at APPEND, so after the switch only rows written from then on ride
  `CONCURRENT`. Rows pending at the restart keep `ORDERED` and drain as before. While the
  two overlap, one aggregate's old ORDERED row and new CONCURRENT row are not ordered
  against each other.
- During a rolling deploy, rows written by a 1.4.x pod still ride `ORDERED`. A row written
  by a 1.5.0 pod carries its lane and ordering key, and a 1.4.x relay honours them, since
  the CONCURRENT claim has read them since 1.3.0. Per-aggregate order is therefore complete
  once every pod that APPENDS runs 1.5.0 with the setting.
- On `CONCURRENT`, Kafka rows share the lane's `concurrent.max-in-flight` slots (default
  8 per pod) with HTTP rows. Keys are taken round-robin, so a slow subscriber cannot
  starve them, but raise the slot count if a pod relays many Kafka rows and slow HTTP
  rows at once.
- The gauges count such rows under `lane="concurrent"`.

## 1.4.0 - 2026-10-08

A cancel no longer accepts a row whose send is in flight, the ops list takes `?state=`,
and the outbox gauges carry a `lane` label.

### Changed

- **The outbox gauges are per lane** (OUTBOX-LANE-GAUGES-1). This changes the METRIC
  SHAPE: re-point dashboards and alerts that read these gauges.
  - `opentmf.outbox.pending`, `in-flight`, `parked` and `relay-lag` each carry a `lane`
    tag, `ordered` or `concurrent`: the lane stamped on the row at append, a row written
    before 1.3.0 counting as `ordered`. Each gauge is now two series instead of one, so
    an alert can name the lane and a slow HTTP subscriber's backlog no longer hides in
    the Kafka lane's numbers (or behind them).
  - The 1.3.0 value is `sum without (lane) (...)`, or `max without (lane) (...)` for
    `relay-lag`. A bare-name query or panel now returns two series; a threshold alert
    such as `opentmf_outbox_parked > 0` keeps working and fires per lane.
  - `metrics-age` and the counters are unchanged.
  - Each refresh still runs four queries; each now answers both lanes in one statement,
    so the two series of a gauge come from one snapshot. Each lane is read through its
    own partial index over the open rows. `OutboxClaimPlanIT` pins every index under
    custom and generic plans, and a case with open rows on both lanes was added.
  - Changeset `006-outbox-lane-gauge-indexes` builds `ix_outbox_parked_lane`,
    `ix_outbox_open_since_ordered` and `ix_outbox_open_since_concurrent` CONCURRENTLY,
    each after a drop-if-exists as in 004/005. It drops `ix_outbox_open_since` (005),
    whose only reader was the lane-less relay-lag gauge. Pending and in-flight need
    nothing new: pending counts the three claim indexes of 004, whose predicates split
    the open rows by lane. An IT proves a database migrated by the released 1.3.0
    changelog upgrades with its checksums intact.

### Added

- **`?state=` on `GET /ops/outbox`** (OUTBOX-STATE-QUERY-1). The plain list now takes
  the derived-state leg as a query parameter (`pending`, `parked`, `relayed`,
  `cancelled`; an unknown value is a 400), the same code path as
  `/ops/outbox/state/{state}`, which stays. The toolkit's filter grammar reads the
  whole query string, so the handler passes `state` through it with
  `@Tmf630PassThrough` (tmf630-toolkit 3.4.0): the exact name only, so `state.eq=` is
  still a filter key (a 400), and every other unknown name is still rejected. The OAS
  fragment documents the parameter.

### Fixed

- **`cancel` accepted a row whose send was in flight** (OUTBOX-CANCEL-REFUSES-LIVE-LEASE-1).
  Since 1.3.0 the relay holds no lock across a send, so a cancel under a live lease
  returned success at once, and a send that then succeeded was booked
  sent-but-cancelled. A caller that took the success as "it will not go out" (dnms-681's
  subscription DELETE did, as it had under 1.2.x, where the cancel waited for the send
  and then refused) was told the effect was withdrawn while it was being delivered.
  - `OutboxMaintenanceService.cancel(id)` now refuses a row under a live lease, on both
    lanes, with the new `OutboxRowInFlightException`. It is an `IllegalStateException`,
    so `POST /ops/outbox/{id}/cancel` answers **409** like the other wrong-state
    refusals; `claimedUntil()` names the lease end. Nothing waits: the refusal is
    immediate.
  - Cancel again once the send is booked: a failed send in backoff is cancellable, a
    delivered one refuses "already relayed".
  - Unclaimed rows and rows whose lease has lapsed stay cancellable. Should a lapsed
    lease's send still book, the booking honours the cancel as before (retires a failed
    send cancelled; books a delivered one sent-but-cancelled), so that state remains,
    narrowed to the lapsed-lease case.

## 1.3.0 - 2026-10-07

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
