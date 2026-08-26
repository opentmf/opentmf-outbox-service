--liquibase formatted sql

--changeset opentmf-outbox:001-outbox
--preconditions onFail:HALT onError:HALT onFailMessage:PostgreSQL is the shipped DDL dialect of opentmf-outbox-service; this changelog must not run against another engine. For other databases see the README (a per-dialect changelog is the extension path). onErrorMessage:PostgreSQL is the shipped DDL dialect of opentmf-outbox-service (the dialect probe errored - this is not a PostgreSQL database). For other databases see the README (a per-dialect changelog is the extension path).
--precondition-sql-check expectedResult:1 SELECT CASE WHEN current_setting('server_version_num') IS NOT NULL THEN 1 ELSE 0 END
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
