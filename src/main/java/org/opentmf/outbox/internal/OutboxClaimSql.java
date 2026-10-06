package org.opentmf.outbox.internal;

/**
 * The CONCURRENT lane's claim, as native SQL - a constant, so {@link OutboxEventRepository} and
 * the plan test ({@code OutboxClaimPlanIT}) run the very same text. Its cost is bounded by the
 * rows it claims plus the keys it has to step over, never by the table, a lane's backlog or one
 * key's backlog:
 *
 * <ul>
 *   <li>{@code in_flight} - the keys under a live lease, read through
 *       {@code ix_outbox_claimed_until} (as many rows as are in flight across pods). A live lease
 *       holds its key even if the row was cancelled meanwhile: its send is still running.
 *   <li>{@code keys} - a skip-scan over {@code ix_outbox_concurrent_keyed}: one index descent per
 *       distinct key, in key order, from {@code :after} (the relay's ROUND-ROBIN cursor) up to
 *       {@code :until} (null = no bound), never reading a key's other rows.
 *   <li>{@code heads} - per key not in flight, its first due row in {@code id} order (one probe),
 *       stopping as soon as {@code :limit} heads are found - the recursion above runs only that
 *       far. Rows of a key behind its head are never read.
 *   <li>{@code unkeyed} - rows without a key, in {@code id} order, over
 *       {@code ix_outbox_concurrent_unkeyed}, at most {@code :unkeyedLimit}.
 *   <li>the outer select joins only the chosen ids (primary-key lookups) and locks them
 *       {@code FOR UPDATE SKIP LOCKED}, re-checking that each is still claimable.
 * </ul>
 *
 * <p>Order: within a key, {@code id} order on the happy path; across keys, round-robin from the
 * cursor - the library promises no order between keys. Keys are stepped over (not claimed) when
 * in flight or when none of their rows is due (all in backoff or held); the in-flight ones are
 * bounded by the lane's slots across pods.
 */
final class OutboxClaimSql {

  static final String CONCURRENT =
      """
      with recursive
        in_flight as (
          select distinct f.ordering_key from outbox f
          where f.claimed_until > :now and f.relayed_on is null
            and f.lane = 'CONCURRENT' and f.ordering_key is not null),
        keys (k) as (
          (select o.ordering_key from outbox o
           where o.lane = 'CONCURRENT' and o.ordering_key is not null
             and o.relayed_on is null and o.cancelled_on is null and o.parked_on is null
             and o.ordering_key > :after
             and (cast(:until as varchar) is null or o.ordering_key <= cast(:until as varchar))
           order by o.ordering_key limit 1)
          union all
          select (select o.ordering_key from outbox o
                  where o.lane = 'CONCURRENT' and o.ordering_key is not null
                    and o.relayed_on is null and o.cancelled_on is null and o.parked_on is null
                    and o.ordering_key > keys.k
                    and (cast(:until as varchar) is null
                      or o.ordering_key <= cast(:until as varchar))
                  order by o.ordering_key limit 1)
          from keys where keys.k is not null),
        heads as (
          select h.id from keys
          cross join lateral (
            select h.id from outbox h
            where h.lane = 'CONCURRENT' and h.ordering_key = keys.k
              and h.relayed_on is null and h.cancelled_on is null and h.parked_on is null
              and (h.release_at is null or h.release_at <= :now)
              and h.next_attempt_on <= :now
              and (h.claimed_until is null or h.claimed_until <= :now)
            order by h.id limit 1) h
          where keys.k is not null
            and not exists (select 1 from in_flight i where i.ordering_key = keys.k)
          limit :limit),
        unkeyed as (
          select u.id from outbox u
          where u.lane = 'CONCURRENT' and u.ordering_key is null
            and u.relayed_on is null and u.cancelled_on is null and u.parked_on is null
            and (u.release_at is null or u.release_at <= :now)
            and u.next_attempt_on <= :now
            and (u.claimed_until is null or u.claimed_until <= :now)
          order by u.id limit :unkeyedLimit)
      select e.* from (
          select id from heads
          union all
          select id from unkeyed
          order by id limit :limit) c
        join outbox e on e.id = c.id
      where e.relayed_on is null and e.cancelled_on is null and e.parked_on is null
        and (e.claimed_until is null or e.claimed_until <= :now)
      order by e.id
      for update of e skip locked""";

  private OutboxClaimSql() {}
}
