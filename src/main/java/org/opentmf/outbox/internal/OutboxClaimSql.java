package org.opentmf.outbox.internal;

/**
 * The relay's claim and booking statements, as native SQL - constants, so
 * {@link OutboxEventRepository} and the plan test ({@code OutboxClaimPlanIT}) run the very same
 * text. The CONCURRENT lane's claim is two statements, chosen by the caller: {@link #CONCURRENT}
 * (keys above the round-robin cursor, plus rows without a key) and {@link #CONCURRENT_WRAP} (keys
 * up to the cursor). Each carries only plain comparisons on its
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
 * <p><strong>Open, as ONE expression (1.9.0, changeset 008).</strong> Every part of these
 * statements that reads open rows says "not relayed, not cancelled, not parked" as {@code
 * coalesce(relayed_on, cancelled_on, parked_on) is null} - the predicate of the two claim indexes
 * ({@code ix_outbox_concurrent_keyed}, {@code ix_outbox_concurrent_unkeyed}) - never as three
 * separate {@code is null}s. The other partial indexes over open rows ({@code ix_outbox_pending},
 * the relay-lag gauge's) say the three separately, so the planner cannot prove their predicate
 * from a claim and never picks them for it, whatever the statistics say. Up to 1.8.0 it could:
 * analysed while the open set was empty, it believed a backlog was one row, walked the keys
 * through {@code ix_outbox_pending} (never analysed: the relay-lag index) and sorted the whole
 * backlog for every key - about 36 s for one claim of 100 rows over 100,000. The in-flight probes
 * keep {@code relayed_on is null}: it is {@code ix_outbox_claimed_until}'s predicate.
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
              and coalesce(h.relayed_on, h.cancelled_on, h.parked_on) is null
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
      where coalesce(e.relayed_on, e.cancelled_on, e.parked_on) is null
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
                 and coalesce(o.relayed_on, o.cancelled_on, o.parked_on) is null
                 and o.ordering_key > :after
               order by o.ordering_key limit 1)
              union all
              select (select o.ordering_key from outbox o
                      where o.lane = 'CONCURRENT' and o.ordering_key is not null
                        and coalesce(o.relayed_on, o.cancelled_on, o.parked_on) is null
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
                and coalesce(u.relayed_on, u.cancelled_on, u.parked_on) is null
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
                 and coalesce(o.relayed_on, o.cancelled_on, o.parked_on) is null
                 and o.ordering_key <= :until
               order by o.ordering_key limit 1)
              union all
              select (select o.ordering_key from outbox o
                      where o.lane = 'CONCURRENT' and o.ordering_key is not null
                        and coalesce(o.relayed_on, o.cancelled_on, o.parked_on) is null
                        and o.ordering_key > keys.k and o.ordering_key <= :until
                      order by o.ordering_key limit 1)
              from keys where keys.k is not null),
          """
          + HEADS
          + """
          select e.* from (select id from heads order by id limit :limit) c
          """
          + LOCK;

  /**
   * The advisory-lock namespace of the CONCURRENT claim's key locks ({@code 'outx'} as an int4):
   * the two-int form keeps the library's locks apart from a consumer's single-bigint locks.
   */
  static final int KEY_LOCK_NAMESPACE = 0x6f757478;

  /**
   * Tries - never waits for - the transaction-scoped advisory lock of EVERY candidate key in ONE
   * statement (1.6.0; one statement per key before), and returns the keys it took. The keys come
   * as an array literal and are unnested, so the statement reads no table at all and the lock is
   * tried exactly once per key. Each key is hashed by the DATABASE ({@code hashtext}), so every pod
   * computes the same lock id whatever library version it runs - a 1.5.x pod's single-key lock and
   * this statement contend for the very same locks. Held until the claim transaction commits, by
   * which time the claim's leases are committed and visible. Two keys hashing alike only share a
   * lock: one of them waits a pass, nothing is ever wrong.
   */
  static final String KEY_LOCKS =
      """
      select k.ordering_key from unnest(cast(:keys as varchar[])) as k (ordering_key)
      where pg_try_advisory_xact_lock(:namespace, hashtext(k.ordering_key))""";

  /**
   * The re-check, in a NEW snapshot taken after the key locks: of the keyed candidates (given as
   * an array literal), those still their key's first due row with no row of their key in flight.
   * Run after the locks, it sees every lease another pod committed before releasing the key's
   * lock - which the first statement's snapshot could not. {@code id = any(...)} reads by primary
   * key; the in-flight probe reads {@code ix_outbox_claimed_until} (a live lease holds its key
   * even on a cancelled row); the lower-due probe reads {@code ix_outbox_concurrent_keyed}. That
   * probe is a correlated SCALAR subquery (1.9.0), never a {@code not exists}: an {@code exists}
   * is flattened into an anti-join, and on a table never analysed the planner then scanned the
   * whole keyed index once per candidate - 10 million buffers, 35 s for 100 candidates over a
   * 100,000-row backlog. A scalar subquery runs per candidate with its key as a parameter, so the
   * key and the id bound are its index condition.
   */
  static final String RECHECK =
      """
      select r.id from outbox r
      where r.id = any (cast(:ids as bigint[]))
        and coalesce(r.relayed_on, r.cancelled_on, r.parked_on) is null
        and (r.release_at is null or r.release_at <= :now)
        and r.next_attempt_on <= :now
        and (r.claimed_until is null or r.claimed_until <= :now)
        and not exists (
          select 1 from outbox f
          where f.claimed_until is not null and f.relayed_on is null
            and f.claimed_until > :now
            and f.lane = 'CONCURRENT' and f.ordering_key = r.ordering_key)
        and (
          select o.id from outbox o
          where o.lane = 'CONCURRENT' and o.ordering_key = r.ordering_key and o.id < r.id
            and coalesce(o.relayed_on, o.cancelled_on, o.parked_on) is null
            and (o.release_at is null or o.release_at <= :now)
            and o.next_attempt_on <= :now
            and (o.claimed_until is null or o.claimed_until <= :now)
          order by o.ordering_key, o.id limit 1) is null""";

  /**
   * Writes the lease of every claimed row that shares one lease end, in ONE statement (1.6.0; one
   * flushed update per row before): {@code claimed_until} and {@code next_attempt_on} - the latter
   * so a pre-1.3.0 relay sees a leased row as not due - and clears {@code sending_since} (1.9.0):
   * a new holder's send has not started yet. The rows are already locked by the claim ({@code FOR
   * UPDATE}); {@code id = any(...)} reads by primary key.
   */
  static final String STAMP =
      """
      update outbox set claimed_until = :stamp, next_attempt_on = :stamp, sending_since = null
      where id = any (cast(:ids as bigint[]))""";

  /**
   * The SEND HEARTBEAT (1.9.0): renews, in ONE statement, the lease of every row this pod is
   * sending whose renewal is due - each guarded on the lease value this holder stamped, so a row
   * another holder took (or a booking cleared) is not touched - and marks each send as under way
   * ({@code sending_since}, kept once set). The rows come as four parallel arrays, unnested
   * together: the ids, the stamps the guard compares, the new lease ends, and the instants the
   * sends started. Returns the ids it renewed; a row missing is lost to this holder. The unnested
   * ids join the table by primary key.
   */
  static final String RENEW_SENDING =
      """
      with renewed as (
        update outbox o set claimed_until = v.renewed, next_attempt_on = v.renewed,
          sending_since = coalesce(o.sending_since, v.since)
        from unnest(cast(:ids as bigint[]), cast(:stamps as timestamptz[]),
            cast(:renewed as timestamptz[]), cast(:since as timestamptz[]))
          as v (id, stamp, renewed, since)
        where o.id = v.id and o.claimed_until = v.stamp
        returning o.id)
      select id from renewed""";

  /**
   * Gives queued rows BACK (1.9.0), in ONE statement: every slot of the pod is busy with a send
   * older than a second, so the rows it claimed into its hand-off queue in the second before would
   * only wait behind them. Each row's lease is cleared - guarded on the stamp this holder wrote, so
   * a row another holder took is not touched - and it is due at once, for any pod with a free slot.
   * The rows come as two parallel arrays, unnested together; they join the table by primary key.
   */
  static final String RELEASE =
      """
      update outbox o set claimed_until = null, next_attempt_on = :now
      from unnest(cast(:ids as bigint[]), cast(:stamps as timestamptz[])) as v (id, stamp)
      where o.id = v.id and o.claimed_until = v.stamp""";

  /**
   * The RELAYED booking in ONE statement (1.6.0; a locking read plus a flushed update before),
   * guarded on the lease this holder stamped: it clears the lease and stamps {@code relayed_on} -
   * and {@code next_attempt_on} with it, no longer the lease end - and returns the row it booked,
   * or nothing when the lease lapsed and another holder re-stamped the row (or booked it). The
   * {@code update} locks the row as the locking read did; a data-modifying {@code with} makes the
   * statement a query, so the booked row comes back as a managed entity for the booking hook and
   * the relayed listeners. A cancelled row is booked all the same - SENT-BUT-CANCELLED.
   */
  static final String BOOK_RELAYED =
      """
      with booked as (
        update outbox set claimed_until = null, sending_since = null, relayed_on = :relayedOn,
          next_attempt_on = :relayedOn
        where id = :id and claimed_until = :stamp
        returning *)
      select * from booked""";

  private OutboxClaimSql() {}
}
