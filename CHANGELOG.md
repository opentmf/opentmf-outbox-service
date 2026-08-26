# Changelog

## 1.0.0 (unreleased)

Initial extraction of the §23 transactional outbox from the dnms-email-adapter
reference implementation.

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
- Library-owned Liquibase changelog (one clean create); library-stable
  `opentmf.outbox.*` metrics; `OutboxArchRules.consumersUseOnlyTheSeams()`
  seal rule (ArchUnit ≥ 1.5.0).
