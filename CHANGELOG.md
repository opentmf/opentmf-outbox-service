# Changelog

## 1.0.0 (unreleased)

Initial release of the transactional outbox as a Spring Boot starter.

- Same-transaction append (`OutboxWriter`, propagation MANDATORY), payload
  frozen at write time; after-commit relay poke + fixed-delay sweep into one
  relay thread per pod; `FOR UPDATE SKIP LOCKED` claim; at-least-once with
  `x-idempotency-key = <service>:outbox:<id>`.
- Derived state (pending / parked / relayed — no status column); exponential
  backoff then park at max-attempts; parked rows never auto-pruned.
- Publisher SPI: Kafka default for non-HTTP destinations (transactional-or-await),
  HTTP publisher for `http(s)://` destinations with per-row `client_profile`
  and the `OutboxClientProfileResolver` seam for named authenticated clients.
- `/ops` surface: prune, unpark break-glass, TMF630 triage list, `/parked`
  derived-state sub-resource, forensic inspect. Consumers own the security rows.
- `OutboxRelayedListener` post-relay seam: consumer bookkeeping inside the claim
  transaction, atomic with `relayed_on`; a throwing listener books an ordinary
  delivery failure and the publish repeats (found during a consumer trial fit).
- Library-owned Liquibase changelog (one clean create); library-stable
  `opentmf.outbox.*` metrics.
- Public contract in `org.opentmf.outbox`; implementation in
  `org.opentmf.outbox.internal` (no compatibility promise);
  `OutboxArchRules.consumersUseOnlyTheSeams()` enforces both the package
  boundary and the no-consumer-repository rule (ArchUnit ≥ 1.5.0).
