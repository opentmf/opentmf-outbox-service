--liquibase formatted sql

--changeset opentmf-outbox:001-outbox
--preconditions onFail:MARK_RAN onError:HALT onFailMessage:outbox table already exists (a pre-library outbox) - changeset 001 marked ran, 003 onboards the table. onErrorMessage:PostgreSQL is the shipped DDL dialect of opentmf-outbox-service (the dialect probe errored - this is not a PostgreSQL database). For other databases see the README (a per-dialect changelog is the extension path).
--precondition-sql-check expectedResult:0 SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'outbox' AND current_setting('server_version_num') IS NOT NULL
-- Transactional outbox — the LIBRARY-OWNED table (opentmf-outbox-service). One per owning
-- service's schema: the
-- business transaction writes its state change AND one row here in the same local transaction;
-- the in-service relay delivers the effect at-least-once. State is DERIVED, no status column:
-- pending = relayed_on is null; parked = pending and attempts >= max-attempts (config, default
-- 10); relayed = relayed_on is not null. No triggers, no stored procedures, no CDC.
-- CONSUMERS include this file from their master changelog and NEVER edit it — schema evolution
-- arrives by library version bump.
-- ONE clean create and nothing else: no onboarding shims — a consumer with a pre-library
-- outbox table owns its own transition.
-- (Comment lines here must never BEGIN with the word "changeset": the strict formatted-SQL
-- parser reads "-- changeset ..." as a malformed header and refuses the whole file.)
create table outbox (
  id              bigint generated always as identity primary key,
  aggregate_type  varchar(64) not null,
  aggregate_id    varchar(128) not null,
  event_type      varchar(100) not null,
  destination     varchar(200) not null,
  client_profile  varchar(64),
  payload         text not null,
  headers         text,
  created_on      timestamp with time zone not null,
  attempts        smallint not null default 0,
  next_attempt_on timestamp with time zone not null,
  relayed_on      timestamp with time zone,
  last_error      text
);

create index ix_outbox_pending on outbox (next_attempt_on) where relayed_on is null;

comment on table outbox is 'Transactional outbox (library-owned, opentmf-outbox-service): effects frozen at commit, relayed at-least-once in id order. Pending = relayed_on is null; parked = pending and attempts >= max-attempts; relayed rows pruned after the configured retention (default 7 days), parked rows never pruned automatically.';
comment on column outbox.id is 'Own identity; the relay order — publishes ascend by id.';
comment on column outbox.aggregate_type is 'Aggregate kind the event belongs to, e.g. party-interaction.';
comment on column outbox.aggregate_id is 'Aggregate identity; becomes the Kafka message key (preserves per-aggregate order).';
comment on column outbox.event_type is 'Payload event type, e.g. comm.outcome.v1; copied into the x-event-type header.';
comment on column outbox.destination is 'Delivery target: a Kafka topic name, or an http(s):// URL for the HTTP publisher.';
comment on column outbox.client_profile is 'Optional named client profile for HTTP delivery; null = resolver decision (longest-prefix base-url match), else plain POST. Ignored by the Kafka publisher.';
comment on column outbox.payload is 'Serialized JSON payload — a genuine blob (TEXT, not jsonb); frozen at write time, never re-read by the platform.';
comment on column outbox.headers is 'Optional serialized header map frozen at write time; the relay stamps x-idempotency-key/x-event-type/x-producer on top.';
comment on column outbox.created_on is 'Row creation time; feeds the relay-lag gauge (age of oldest pending).';
comment on column outbox.attempts is 'Failed delivery attempts so far; at max-attempts the row parks (no more retries, alert fires, unparking is an explicit ops action).';
comment on column outbox.next_attempt_on is 'Earliest next delivery attempt (exponential backoff: base 5s, factor 2, cap 10min by default); now() on insert.';
comment on column outbox.relayed_on is 'Delivery completion time; null while pending/parked — the derived-state discriminator.';
comment on column outbox.last_error is 'Last delivery failure (truncated) — ops forensics for parked rows.';
comment on index ix_outbox_pending is 'Partial index over pending rows only — the relay claim query stays cheap regardless of relayed backlog.';
--rollback drop table outbox;

--changeset opentmf-outbox:002-outbox-hold-and-cancel
--preconditions onFail:MARK_RAN onError:HALT onFailMessage:release_at / cancelled_on already exist (a pre-library outbox) - changeset 002 marked ran, 003 adds whichever is missing.
--precondition-sql-check expectedResult:0 SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'outbox' AND column_name IN ('release_at', 'cancelled_on')
-- 1.1.0, ADDITIVE (001 is never edited): the scheduled-send HOLD and the cancellation of an
-- unreleased effect. Both nullable; a 1.0.0 consumer upgrades with zero changes. The state
-- model gains its fourth leg: cancelled = cancelled_on is not null (only unrelayed rows can be
-- cancelled, so relayed and cancelled never overlap); pending/parked now also require
-- cancelled_on is null. Neither column is touched by the retry backoff - the hold is frozen at
-- write time and the failure bookkeeping lives in next_attempt_on alone.
alter table outbox add column release_at timestamp with time zone;
alter table outbox add column cancelled_on timestamp with time zone;

comment on table outbox is 'Transactional outbox (library-owned, opentmf-outbox-service): effects frozen at commit, relayed at-least-once in id order. Pending = relayed_on is null and cancelled_on is null; parked = pending and attempts >= max-attempts; relayed = relayed_on is not null; cancelled = cancelled_on is not null. A pending row with a future release_at is HELD (not claimable until then). Relayed and cancelled rows pruned after the configured retention (default 7 days), parked rows never pruned automatically.';
comment on column outbox.release_at is 'Optional scheduled-send hold, frozen at write time: the row is not claimable before this instant. Null = no hold. Never moved by the retry backoff (that is next_attempt_on).';
comment on column outbox.cancelled_on is 'Cancellation time of an UNRELEASED effect (ops action); null = not cancelled. A cancelled row is never relayed, is retained for audit and pruned with the relayed retention.';
--rollback alter table outbox drop column cancelled_on;
--rollback alter table outbox drop column release_at;

--changeset opentmf-outbox:003-outbox-policy-reference-onboarding
-- 1.2.0, ADDITIVE and IDEMPOTENT (every ADD COLUMN is IF NOT EXISTS): (a) parked_on - the
-- explicit park stamp, now that the attempt budget is per publisher and "parked" can no longer
-- be derived from attempts >= one max; (b) reference - the private per-row correlation, never
-- a wire header; (c) ONBOARDING of a pre-library outbox table: 001/002 were MARKED RAN because
-- the table (or its columns) pre-existed, and this changeset brings such a table to the 1.2.0
-- shape - client_profile / release_at / cancelled_on included for tables that never had them.
-- Consumer-specific deltas stay the consumer's (e.g. dropping a NOT NULL / DEFAULT on
-- release_at, or columns the library does not know). The pending index is recreated to the
-- 1.2.0 claim predicate. (001's own comment block still says "no onboarding shims": it is
-- HISTORICAL and stays verbatim because a released changeset's body - comments included - is
-- part of its recorded checksum; this changeset is the onboarding.) The 001/002 preconditions
-- probe information_schema for current_schema(): the outbox lives in the consumer's own schema.
alter table outbox add column if not exists client_profile varchar(64);
alter table outbox add column if not exists release_at timestamp with time zone;
alter table outbox add column if not exists cancelled_on timestamp with time zone;
alter table outbox add column if not exists parked_on timestamp with time zone;
alter table outbox add column if not exists reference varchar(128);

drop index if exists ix_outbox_pending;
create index ix_outbox_pending on outbox (next_attempt_on) where relayed_on is null and cancelled_on is null and parked_on is null;

comment on table outbox is 'Transactional outbox (library-owned, opentmf-outbox-service): effects frozen at commit, relayed at-least-once in id order. Pending = relayed_on is null and cancelled_on is null; parked = pending and parked_on is not null (a publisher''s delivery budget exhausted with outcome PARK); relayed = relayed_on is not null (also stamped by a DROP exhaustion - last_error tells); cancelled = cancelled_on is not null. A pending row with a future release_at is HELD (not claimable until then). Relayed and cancelled rows pruned after the configured retention (default 7 days), parked rows never pruned automatically.';
comment on column outbox.client_profile is 'Optional named client profile for HTTP delivery; null = resolver decision (longest-prefix base-url match), else plain POST. Ignored by the Kafka publisher.';
comment on column outbox.release_at is 'Optional scheduled-send hold, frozen at write time: the row is not claimable before this instant. Null = no hold. Never moved by the retry backoff (that is next_attempt_on).';
comment on column outbox.cancelled_on is 'Cancellation time of an UNRELEASED effect (ops action); null = not cancelled. A cancelled row is never relayed, is retained for audit and pruned with the relayed retention.';
comment on column outbox.parked_on is 'Stamped when the publisher''s delivery budget is exhausted with outcome PARK: unclaimable, never auto-pruned, the parked gauge alerts; unpark clears it (ops action). Null while retrying.';
comment on column outbox.reference is 'Optional PRIVATE correlation (e.g. a subscription id): filterable on the ops list, never forwarded to the wire - wire headers live in headers.';
comment on column outbox.attempts is 'Failed delivery attempts so far; at the publisher''s budget (library max-attempts by default) the row is exhausted: parked (parked_on) or dropped (relayed_on + last_error).';
comment on index ix_outbox_pending is 'Partial index over claimable-candidate rows only (not relayed, not cancelled, not parked) - the relay claim query stays cheap regardless of the terminal backlog.';
--rollback drop index if exists ix_outbox_pending;
--rollback create index ix_outbox_pending on outbox (next_attempt_on) where relayed_on is null;
--rollback alter table outbox drop column if exists reference;
--rollback alter table outbox drop column if exists parked_on;
