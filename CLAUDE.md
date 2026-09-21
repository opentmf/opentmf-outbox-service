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

## Open

- PR #4 (`feat/ops-oas-fragment`): the /ops/outbox OAS fragment shipped in the
  jar — open proposal, awaiting review; not stale.
- `?state=` on `GET /ops/outbox` needs a tmf630-toolkit pass-through allowance;
  the ruled path form `/ops/outbox/state/{state}` ships since 1.2.0.
- Develop is `1.2.2-SNAPSHOT`; a new CHANGELOG section uses the bare numeric
  heading `## 1.2.2 - <date>`.
