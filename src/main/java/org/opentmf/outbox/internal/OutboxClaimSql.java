package org.opentmf.outbox.internal;

/**
 * The CONCURRENT lane's claim, as native SQL - constants, so {@link OutboxEventRepository} and
 * the plan test ({@code OutboxClaimPlanIT}) run the very same text. Two statements, chosen by the
 * caller: {@link #CONCURRENT} (keys above the round-robin cursor, plus rows without a key) and
 * {@link #CONCURRENT_WRAP} (keys up to the cursor). Each carries only plain comparisons on its
 * key bound - never an {@code OR} on a parameter - so the key range is an index condition under a
 * custom and a generic plan alike; the shape is the caller's, the planner is never relied on.
 *
 * <p>The cost is bounded by the rows claimed plus the keys and rows stepped over, never by the
 * table, the relayed history or one key's backlog:
 *
 * <ul>
 *   <li>{@code in_flight} - the keys under a live lease, read through
 *       {@code ix_outbox_claimed_until} (as many rows as are in flight across pods) into ONE
 *       array, computed once; each key is tested against it with a plain {@code <> all(...)}.
 *       Not a join: a hash anti-join (a generic plan's choice) would read every key and lose the
 *       early stop. A live lease holds its key even if the row was cancelled meanwhile.
 *   <li>{@code keys} - a skip-scan over {@code ix_outbox_concurrent_keyed}: one index descent per
 *       distinct key, in key order, above {@code :after} (first pass) or up to {@code :until}
 *       (wrap pass), never reading a key's other rows.
 *   <li>{@code heads} - per key not in flight, its first due row in {@code id} order (one probe),
 *       stopping as soon as {@code :limit} heads are found - the recursion above runs only that
 *       far. Rows of a key behind its head are never read.
 *   <li>{@code unkeyed} (first pass only) - rows without a key, in {@code id} order, over
 *       {@code ix_outbox_concurrent_unkeyed}.
 *   <li>the outer select joins only the chosen ids (primary-key lookups) and locks them
 *       {@code FOR UPDATE SKIP LOCKED}, re-checking the WHOLE eligibility predicate on each - a
 *       row another pod claimed, sent, failed and booked into backoff since the snapshot is not
 *       taken again.
 * </ul>
 *
 * <p>Stepped over, not claimed: keys in flight (bounded by the lane's slots across pods), keys
 * whose rows are all in backoff or held, and unkeyed rows in backoff or held - the claim's cost
 * grows with those (measured in the README). Order: within a key, {@code id} order on the happy
 * path; across keys, round-robin from the cursor - the library promises no order between keys.
 * {@code ordering_key} is {@code collate "C"}, so the database's key order and the cursor's (Java
 * {@link String} order) agree for every key without supplementary characters.
 */
final class OutboxClaimSql {

  private static final String IN_FLIGHT =
      """
      with recursive
        in_flight as (
          select coalesce(array_agg(distinct f.ordering_key), '{}') as keys from outbox f
          where f.claimed_until > :now and f.relayed_on is null
            and f.lane = 'CONCURRENT' and f.ordering_key is not null),
      """;

  private static final String HEADS =
      """
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
            and keys.k <> all (cast((select i.keys from in_flight i) as varchar[]))
          limit :limit)
      """;

  private static final String LOCK =
      """
        join outbox e on e.id = c.id
      where e.relayed_on is null and e.cancelled_on is null and e.parked_on is null
        and (e.release_at is null or e.release_at <= :now)
        and e.next_attempt_on <= :now
        and (e.claimed_until is null or e.claimed_until <= :now)
      order by e.id
      for update of e skip locked""";

  /** First pass: keys ABOVE {@code :after} (the cursor; {@code ""} = all), plus unkeyed rows. */
  static final String CONCURRENT =
      IN_FLIGHT
          + """
            keys (k) as (
              (select o.ordering_key from outbox o
               where o.lane = 'CONCURRENT' and o.ordering_key is not null
                 and o.relayed_on is null and o.cancelled_on is null and o.parked_on is null
                 and o.ordering_key > :after
               order by o.ordering_key limit 1)
              union all
              select (select o.ordering_key from outbox o
                      where o.lane = 'CONCURRENT' and o.ordering_key is not null
                        and o.relayed_on is null and o.cancelled_on is null and o.parked_on is null
                        and o.ordering_key > keys.k
                      order by o.ordering_key limit 1)
              from keys where keys.k is not null),
          """
          + HEADS
          + """
          ,
            unkeyed as (
              select u.id from outbox u
              where u.lane = 'CONCURRENT' and u.ordering_key is null
                and u.relayed_on is null and u.cancelled_on is null and u.parked_on is null
                and (u.release_at is null or u.release_at <= :now)
                and u.next_attempt_on <= :now
                and (u.claimed_until is null or u.claimed_until <= :now)
              order by u.id limit :limit)
          select e.* from (
              select id from heads
              union all
              select id from unkeyed
              order by id limit :limit) c
          """
          + LOCK;

  /** Wrap pass: keys from the first UP TO {@code :until} (the cursor), no unkeyed rows. */
  static final String CONCURRENT_WRAP =
      IN_FLIGHT
          + """
            keys (k) as (
              (select o.ordering_key from outbox o
               where o.lane = 'CONCURRENT' and o.ordering_key is not null
                 and o.relayed_on is null and o.cancelled_on is null and o.parked_on is null
                 and o.ordering_key <= :until
               order by o.ordering_key limit 1)
              union all
              select (select o.ordering_key from outbox o
                      where o.lane = 'CONCURRENT' and o.ordering_key is not null
                        and o.relayed_on is null and o.cancelled_on is null and o.parked_on is null
                        and o.ordering_key > keys.k and o.ordering_key <= :until
                      order by o.ordering_key limit 1)
              from keys where keys.k is not null),
          """
          + HEADS
          + """
          select e.* from (select id from heads order by id limit :limit) c
          """
          + LOCK;

  private OutboxClaimSql() {}
}
