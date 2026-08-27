# opentmf-outbox-service — Claude session context

**What this is.** THE §23 transactional-outbox library of the DNMS estate
(`org.opentmf.util`, Java-17 baseline, PUBLIC repo — keep customer/VFDE specifics
out). 1.0.0 released 2026-08-26 (tag `opentmf-outbox-service-1.0.0`); pinned in
opentmf-versions BOM 2.1.20. Hand-written outboxes in services are legacy —
they migrate to this at their next touch.

## Ties (load-bearing)

- **THE engineering standard**: `combined-rules.md` in the PRIVATE analysis repo —
  `/home/gokhan/prj/dnotify/analysis/to-be-resources/plan-rules/combined-rules.md`
  (§23 = the outbox standard this library implements; §3 pinning; §19 release).
- **The task definition**: the analysis repo's
  `to-be-resources/opentmf-outbox/implementation-plan.md` — read its **§1.1.0**
  block first. The analysis repo is private forever: **never link into it from
  anything published here**; restate content instead.
- **Never push without Gökhan's explicit word.** `git fetch` before any
  ahead/behind claim. Central publishing is Gökhan's (namespace verified under
  his Portal account) — the session prepares, he cuts.

## CURRENT TASK — 1.2.0, the LAST gap-closing release (spec: analysis plan §1.2.0; IMPLEMENTED, awaiting review)

Branch `feature/last-gap-closing` (local, unpushed — pushes only on Gökhan's
word), one commit on top of develop (1.1.0 released 2026-08-27). Gate green
(57 unit + 13 IT incl. the three consumer-conformance ITs, JaCoCo met, Sonar 0
open / gate OK). Ready ping sent with the item-by-item conformance table.
E-state shipped as the ruled path form `GET /ops/outbox/state/{state}`.
Build recipe: `JAVA_HOME=/usr/lib/jvm/openjdk-17 mvn -Psonar clean verify`
(the enforcer refuses the default JDK 25).

1.0.0 shipped without two plan-mandated fields. 1.1.0 adds both, **additively**:

| Column | Meaning |
|---|---|
| `release_at` (nullable) | the scheduled-send HOLD — rows with a future `release_at` are not eligible (dnms-681's Q2 pattern; a plan-conformance omission the 1.0.0 review missed) |
| `cancelled_on` (nullable) | cancellation of an UNRELEASED effect (dnms-681's §7.7 cancel path, ruled GENERIC) |

- **Liquibase changeset 002** (additive; `src/main/resources/db/changelog/`).
- **Claim predicate** becomes
  `relayed_on IS NULL AND cancelled_on IS NULL AND (release_at IS NULL OR release_at <= now) AND …`.
- **Backoff NEVER touches the hold** — a NAMED regression test proves a retry
  reschedule cannot move `release_at`.
- Writer/ops API surface gains the two fields where a caller sets them; readers
  stay tolerant (additive evolution).
- CHANGELOG: bare-numeric `## [1.1.0]` section; document the change by its
  EFFECT for consumers (hold + cancel become possible), not by the diff.
- Then: Gökhan cuts 1.1.0 → BOM 2.1.21 → dnms-681's migration proceeds.

**Process rule adopted from this omission:** reviews carry a plan-conformance
checklist — every enumerable plan surface diffed item-by-item; an omission is a
finding, not a follow-up.
