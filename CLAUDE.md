# opentmf-outbox-service — Claude session context

**What this is.** The transactional-outbox library of the opentmf estate
(`org.opentmf.util`, Java-17 baseline, Spring Boot 4.x parent). PUBLIC repo — keep
customer specifics out. Consumers pick it up through the opentmf-versions BOM; a
library never imports that BOM (direct property pins in the pom).

## Standing rules

- **Never push without Gökhan's explicit word.** `git fetch` before any
  ahead/behind claim. Release cuts happen on his typed go: `JAVA_HOME=/usr/lib/jvm/openjdk-17
  mvn -B release:prepare release:perform` (release profile via `releaseProfiles`,
  Central auto-publish), then read the GAV back from repo1.maven.org.
- **Build recipe:** `JAVA_HOME=/usr/lib/jvm/openjdk-17 mvn -Pmutation,sonar clean verify`
  (the enforcer refuses the default JDK 25; ITs need Docker — Testcontainers
  Postgres + Kafka; the local SonarQube compose stack must be up). `-Pmutation`
  only defines pitest — PIT figures come from `mvn -Pmutation pitest:mutationCoverage`;
  the README's "PIT survivor review" lists the accepted survivors.
- **Readiness** = gate green, both `versions:display-property-updates` and
  `display-plugin-updates` online with `-Pmutation,sonar,release` and the estate
  ignore string (the pom's own `<maven.version.ignore>`), Sonar 0 open, tree clean.
  No container image, so no image scan. pitest 1.19.6 is the one deliberate hold.
  A release bumps EVERY version that has a newer release (parent check too).
- **The gate is local only** (no CI workflow; the SonarCloud check builds nothing).
  The `virtual-threads-it` failsafe execution needs a JDK 21+ entry in
  `~/.m2/toolchains.xml`. Failsafe MERGES into an existing summary file: a loop of
  runs without `clean` must delete `target/failsafe-reports/failsafe-summary*.xml`.
- **Claim / gauge / prune / unpark SQL is native and plan-pinned** (`OutboxClaimSql`,
  `OutboxGaugeSql`, `OutboxPruneSql`, `OutboxUnparkSql`; `OutboxClaimPlanIT` runs every
  statement PREPAREd under `force_custom_plan` AND `force_generic_plan`). Fix plans by
  index and query SHAPE only - no hints, no `plan_cache_mode` in the library.
- **Merge convention:** PR to `develop`, merged as a merge commit, branch deleted.
  Identity `gokhanus`; no AI attribution lines in this repo.
- **Reviews carry a plan-conformance checklist:** every enumerable plan surface is
  diffed item-by-item; an omission is a finding, not a follow-up.

## Release history (details in CHANGELOG.md)

- 1.0.0 (2026-08-26), 1.1.0 (2026-08-27: `release_at` hold + `cancelled_on`),
  1.2.0 (2026-08-27: per-publisher failure policy, `parked_on`, `reference`,
  onboarding changesets, ops parity, consumer-conformance ITs).
- **1.2.1 (2026-09-21)** — a Kafka-less consumer must start. Plan and the two
  as-performed deviations: `docs/KAFKA_LESS_STARTUP_PLAN.md` §4. The publisher
  defaults are nested, name-guarded, stereotype-free member classes of
  `OutboxAutoConfiguration`; `KafkaLessStartupTests` pins the shape with a
  child-first classloader (Boot's `FilteredClassLoader` cannot reproduce the
  reflective failure).
- **1.3.0 (2026-10-07)** — OUTBOX-HTTP-LANE-1 (#6): ORDERED / CONCURRENT lanes stamped at
  append, claim by LEASE (`claimed_until`), booking hook for every outcome, ordering keys
  never in flight together within a pod or across pods (PostgreSQL advisory lock +
  re-check in a new snapshot; relay tx READ COMMITTED). OUTBOX-GAUGES-AND-PRUNE-AT-SCALE-1
  (#7, load-test F-1/F-2): gauges from a refreshed snapshot (`metrics-age`), set-based
  bounded prune (`REQUIRES_NEW` batches), unpark by filter. The ops OAS fragment (#4).
  Changesets 004/005 build their indexes CONCURRENTLY.
- **1.4.0 (2026-10-08)** — no 1.3.1 (Gökhan's ruling): cancel REFUSES a row under a live
  lease (`OutboxRowInFlightException`, 409; #8); `GET /ops/outbox?state=` via
  `@Tmf630PassThrough` beside the path form (#9); the four row gauges tagged by `lane` - a
  metric-shape change - with changeset 006's per-lane gauge indexes (#10).
- **1.5.0 (2026-10-09)** - OUTBOX-KAFKA-PER-KEY-ORDER-1 (#11): `opentmf.outbox.kafka.lane`
  ORDERED (default) | CONCURRENT, `kafka.ordering-key` AGGREGATE_ID | NONE, `kafka.lease`
  (> send-timeout); record key stays the raw aggregateId. `OutboxKafkaTwoPodsIT` (two contexts,
  one DB, one broker) is red on `-Dit.kafka.lane=ORDERED` only with rows contiguous per aggregate
  and one shared `releaseAt` - keep that shape. Push gate: `sonar-local-zero <branch>
  opentmf_opentmf-outbox-service-local` must exit 0 on an analysis of HEAD.

- **1.6.0 (2026-10-13)** - OUTBOX-RELAY-CLAIM-CADENCE-1 (#12): the CONCURRENT
  claim runs ahead of the slots into a hand-off queue of `batch-size`; one statement each for
  the key locks (`KEY_LOCKS`), the stamp (`STAMP`) and the RELAYED booking (`BOOK_RELAYED`);
  a queued row that waited 1/10 of its lease is renewed before its send; the ORDERED claim
  runs only when ORDERED rows may be due; the CONCURRENT lane's bookings run at the app's
  default isolation (claims + ORDERED bookings keep the RC pin - coordinator ruling).
  `OutboxRelayThroughputIT` (Toxiproxy 5 ms per statement, `-Dit.relay.latency-ms`) is the
  golden test: >= 500 rows/s at 8 slots, 10k rows < 15 s at 64; `OutboxLaneBookingStatementsIT`
  pins the booking's statements from PostgreSQL's own log.
- **1.7.0 (2026-10-15)** - the ops contract as the first black-box suites found it (#13):
  OUTBOX-OAS-LIMIT-CLAMP-1 (`limit` declares no `maximum`; the toolkit clamps),
  OUTBOX-OPS-ID-PATH-DIGITS-1 (`{id:\d+}` on the three row routes: `GET /ops/outbox/unpark`
  is a 405, a non-digit segment a 404), OUTBOX-OAS-FRAGMENT-SECURITY-RESPONSES-1 (every
  operation `$ref`s 401/403/405/406/503 + 415 on the bulk unpark; every consumer-rendered
  error carries `OutboxProblem` as problem+json; the six consumer answers + `OutboxProblem`
  are `x-opentmf-refinable: true` - redefinable under the same name, the rest verbatim).
  `OpsFragmentConformance` checks status AND written media type against the fragment
  (`OutboxOpsWireConformanceTests`, `OutboxOpsProblemDetailsIT` with Boot problem details and
  the toolkit guard on REJECT). tmf630-toolkit 3.5.1, central-publishing 0.12.0. A new error
  answer on the ops surface must land in the fragment, or `OpsFragmentConformance` is red.
- **1.8.0 (2026-10-16)** - #14: OUTBOX-CONCURRENT-CLAIM-INDEPENDENT-1 (`OutboxOrderedLane`, thread
  `opentmf-outbox-ordered`, sends ORDERED batches; the relay thread only claims; ONE ORDERED batch
  per pod - the relay claims ORDERED only while the lane is idle, the lane's onIdle nudges it;
  `OutboxLaneIndependenceIT`; pool `max-in-flight` + 3), OUTBOX-FRAGMENT-ERROR-MEDIA-TYPE-REFINABLE-1
  (every error in the paths a `$ref`; `x-opentmf-refinable: content` on the 8 library refusals =
  description + headers verbatim, content the consumer's; `true` still exactly the 6 consumer
  answers + `OutboxProblem`; `OutboxOpsFragmentDriftRuleTests` is the drift rule as code),
  OUTBOX-UNPARK-FILTER-STRICT-TYPES-1 (body bound as a JSON object, `UnparkFilter.from` types it -
  never the consumer's mapper, never coerced). `OutboxClaimPlanIT`'s idle gauges pin open-row
  indexes + a buffer bound, not an index: two EMPTY partial indexes tie in the planner.

## Open

- Develop is `1.8.1-SNAPSHOT`; a new CHANGELOG section uses the bare numeric
  heading `## 1.8.1 - <date>` (or the next minor's, if the row is a minor).
- Nothing open. OUTBOX-UNUSED-INDEX-1 was closed by Gökhan on 2026-10-15: keep
  `ix_outbox_pending` (measured the most-scanned index on every outbox table).
