# opentmf-outbox-service

Transactional outbox (§23) as a Spring Boot starter: library-owned table DDL
(Liquibase), an at-least-once relay with pluggable publishers (Kafka default,
HTTP for `http(s)://` destinations), and an `/ops` maintenance surface
(prune / unpark / TMF630 list).

## Guarantees and mechanism

- **Never dual-write.** `OutboxWriter.append(...)` runs with propagation
  `MANDATORY` — the outbox row commits or rolls back with the caller's business
  transaction, never in one of its own. The payload is serialized at write
  time: the event is a fact frozen at commit.
- **At-least-once, consumer-dedupable.** The relay stamps
  `x-idempotency-key = <service>:outbox:<id>` (plus `x-event-type`,
  `x-producer`) on every delivery. A crash between delivery and commit means
  redelivery; the key makes consumer dedup trivial.
- **One relay thread per pod, `FOR UPDATE SKIP LOCKED` across pods.** Two
  triggers feed one single-threaded executor: an after-commit poke (normal
  path, milliseconds) and a fixed-delay sweep (default 5s — timers are for the
  tail). Each pass drains while full batches come back.
- **Backoff, then park.** A failed row books `attempts++`, `last_error`, and an
  exponential next attempt (5s base, factor 2, 10min cap). At `max-attempts`
  (default 10) the row **parks**: excluded from claims, `opentmf.outbox.parked`
  gauge alerts, unparking is an explicit ops action. Parked rows are never
  auto-pruned.
- **Derived state, no status column.** pending = `relayed_on is null`;
  parked = pending and `attempts >= max-attempts`; relayed = `relayed_on set`.
  There is no state machine to corrupt.

## Five-minute adoption

1. **Dependency** (via the `opentmf-versions` BOM once released):

   ```xml
   <dependency>
     <groupId>org.opentmf.util</groupId>
     <artifactId>opentmf-outbox-service</artifactId>
   </dependency>
   ```

2. **DDL** — include the library-owned changelog by reference from your master
   changelog (never copy it):

   ```xml
   <include file="classpath:db/changelog/opentmf-outbox.sql"/>
   ```

3. **Write** — inside your business transaction:

   ```java
   outboxWriter.append("party-interaction", aggregateId, "comm.outcome.v1",
       "comm.delivery.v1", factObject);
   ```

   Kafka topic or `http(s)://` URL both go in `destination`; the publisher SPI
   routes by shape. The relay, metrics, and `/ops` endpoints auto-configure.

4. **Configuration** (`opentmf.outbox.*`): `sweep-interval` 5s, `batch-size`
   100, `max-attempts` 10, `backoff-base` 5s / `backoff-factor` 2 /
   `backoff-cap` 10m, `retention` 7d, `send-timeout` 10s,
   `ops-endpoints` true.

## Publisher SPI

`OutboxPublisher` (`supports`/`publish`) — first-supports-wins, the Kafka
publisher registers at lowest precedence as the default for every non-HTTP
destination. HTTP destinations POST the payload with the §14 relay headers.

Per-row named-client selection (the 2026-08-26 hub ruling): pass
`clientProfile` on the append overload, and implement
`OutboxClientProfileResolver` to map profile/destination to a configured
`RestClient` (e.g. an `opentmf-http-clients` named profile, longest-prefix
base-url match). No resolver or no match = the plain default client. The
library deliberately does **not** depend on `opentmf-http-clients` — consumers
bring their own client stack behind the resolver seam.

## Ops surface (`/ops`, network-allowlisted)

- `GET /ops/outbox` — TMF630 toolkit list (Querydsl predicate + paging,
  strict unknown-field 400). Payloads are omitted in lists.
- `GET /ops/outbox/parked` — the derived-state sub-resource (a config
  comparison the wire cannot express).
- `GET /ops/outbox/{id}` — full forensic row, payload and `last_error`.
- `POST /ops/outbox/{id}/unpark` — break-glass after the root cause is fixed:
  resets attempts, makes the row due now, nudges the relay.
- `POST /ops/outbox/maintenance/prune` — deletes relayed rows past retention
  (wire this to the §25.5 CronJob kicker).

Disable the endpoints with `opentmf.outbox.ops-endpoints=false`.

## The seal rule

Business code touches the outbox only through the public seams. Add one line to
your ArchUnit suite:

```java
OutboxArchRules.consumersUseOnlyTheSeams().check(importedClasses);
```

It forbids consumer-owned Spring Data repositories over the `OutboxEvent`
entity — the one misuse the compiler cannot stop (the library repository
itself is package-private). Requires ArchUnit ≥ 1.5.0 on Java 25 bytecode.

## Migrating from a hand-written outbox

1. Delete your own `outbox` table changeset **before your first release** (or
   write a rename/migrate changeset after it); include the library changelog
   instead. Column set matches the §23 shape plus `client_profile`.
2. Replace your writer/relay/park classes with `OutboxWriter` + configuration.
   Keep your `x-idempotency-key` format — the library's is the ruled
   `<service>:outbox:<id>`.
3. Swap dashboard/alert queries to the library-stable metric names
   (`opentmf.outbox.pending|parked|relay-lag|relayed|attempts`) — services are
   distinguished by registry tags, never per-service metric prefixes.
4. Point ops runbooks at the `/ops` trio; direct DB reads are a missing-module
   smell (no-direct-DB rule).

## Quality gates

- JaCoCo bundle gate **90/90/90** (line/instruction/branch), unit + IT merged
  (the higher of the dnms and opentmf floors). Generated Querydsl classes are
  excluded from the denominator.
- PIT (`mvn -Pmutation test-compile org.pitest:pitest-maven:mutationCoverage`)
  on the ruled 1.19.6 hold with incremental history — see the pin comment in
  the pom. No mutation threshold: survivor review is the unit of work.

### PIT survivor review (2026-08-26, 123 mutations, 97 killed)

Accepted survivors, each reviewed:

| Where | Mutant | Verdict |
|---|---|---|
| `OutboxMaintenanceService.pruneRelayed` | `pruned > 0` boundary/negation | Log-only guard; row deletion is unaffected |
| `OutboxRelayWorker.registerFailure` | `attempts >= max` boundary/negation | Log-level-only; parked state is *derived* from attempts, not this branch |
| `OutboxRelayWorker.truncate` | `<=` vs `<` boundary | Equivalent mutant at exactly 4000 chars |
| `OutboxRelay.stop` | awaitTermination conditional | Shutdown-timing leg; a kill needs a 5s hanging-task test for no insight |
| `OutboxRelay` thread factory | removed `setDaemon` | Asserted by `OutboxRelayTests` in every normal run; PIT's per-line selection misses the factory-lambda mapping |

`NO_COVERAGE` entries (auto-configuration bean methods, ops controller) are
exercised by the Testcontainers ITs, which the estate posture deliberately
keeps out of PIT (unit tests only).
