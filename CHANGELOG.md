# Changelog

## 1.1.0 (unreleased)

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
