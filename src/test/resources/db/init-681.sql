-- A dnms-681-shaped PRE-LIBRARY outbox table, as its own changesets left it: the library's
-- 001/002 must MARK_RAN over it and 003 must add what is missing. release_at is NOT NULL with
-- a default (the day-one insert killer), and it carries columns the library does not know.
create table outbox (
  id              bigint generated always as identity primary key,
  aggregate_type  varchar(64) not null,
  aggregate_id    varchar(128) not null,
  event_type      varchar(100) not null,
  destination     varchar(200) not null,
  payload         text not null,
  headers         text,
  created_on      timestamp with time zone not null,
  attempts        smallint not null default 0,
  next_attempt_on timestamp with time zone not null,
  release_at      timestamp with time zone not null default now(),
  relayed_on      timestamp with time zone,
  cancelled_on    timestamp with time zone,
  last_error      text,
  kind            varchar(32),
  subscription_id varchar(64)
);
create index ix_outbox_pending on outbox (next_attempt_on) where relayed_on is null;
