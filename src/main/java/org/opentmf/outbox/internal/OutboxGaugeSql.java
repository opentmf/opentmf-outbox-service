package org.opentmf.outbox.internal;

/**
 * The gauge queries, as native SQL constants - run by {@link OutboxEventRepository} and pinned
 * by {@code OutboxClaimPlanIT} with the very same text. Each answers BOTH lanes in one statement
 * (one snapshot), as the columns {@code ordered} and {@code concurrent} (1.4.0) - the ORDERED
 * lane being every row not stamped CONCURRENT, a null lane included, exactly as the claims read
 * it. None reads the relayed part of the table; each lane's part is served by a partial index
 * whose predicate it repeats:
 *
 * <ul>
 *   <li>{@link #OPEN_NOT_PARKED} - the three claim indexes, whose predicates split the open,
 *       unparked rows by lane: {@code ix_outbox_ordered_claim}, {@code ix_outbox_concurrent_keyed}
 *       and {@code ix_outbox_concurrent_unkeyed} (index-only counts);
 *   <li>{@link #PARKED} - {@code ix_outbox_parked_lane}, one index-only count per lane;
 *   <li>{@link #OPEN_SINCE} - {@code ix_outbox_open_since_ordered} and
 *       {@code ix_outbox_open_since_concurrent}, each one's first entry: the instant the oldest
 *       open row of the lane became deliverable ({@code greatest(created_on, release_at)}) - a
 *       held row's instant lies in the future, so it lags only once its hold has passed;
 *   <li>{@link #IN_FLIGHT} - {@code ix_outbox_claimed_until}: its WHERE is exactly that index's
 *       predicate and the time and lane tests sit in the aggregates' FILTERs - a {@code
 *       claimed_until > :now} in the WHERE leaves a generic plan blind to how few rows it
 *       selects, and it then picked another index and filtered every open row.
 * </ul>
 *
 * <p>Their cost grows with the OPEN rows (pending, parked, in flight), never with the relayed
 * history. They run on the metrics refresh schedule, never on a scrape.
 */
final class OutboxGaugeSql {

  static final String OPEN_NOT_PARKED =
      """
      select
        (select count(*) from outbox
         where (lane is null or lane <> 'CONCURRENT')
           and relayed_on is null and cancelled_on is null and parked_on is null) as ordered,
        (select count(*) from outbox
         where lane = 'CONCURRENT' and ordering_key is not null
           and relayed_on is null and cancelled_on is null and parked_on is null)
        + (select count(*) from outbox
           where lane = 'CONCURRENT' and ordering_key is null
             and relayed_on is null and cancelled_on is null and parked_on is null) as concurrent""";

  static final String PARKED =
      """
      select count(*) filter (where lane is null or lane <> 'CONCURRENT') as ordered,
        count(*) filter (where lane = 'CONCURRENT') as concurrent
      from outbox
      where parked_on is not null and relayed_on is null and cancelled_on is null""";

  static final String OPEN_SINCE =
      """
      select
        (select min(greatest(created_on, coalesce(release_at, created_on))) from outbox
         where (lane is null or lane <> 'CONCURRENT')
           and relayed_on is null and cancelled_on is null) as ordered,
        (select min(greatest(created_on, coalesce(release_at, created_on))) from outbox
         where lane = 'CONCURRENT' and relayed_on is null and cancelled_on is null) as concurrent""";

  static final String IN_FLIGHT =
      """
      select count(*) filter (where claimed_until > :now and cancelled_on is null
          and (lane is null or lane <> 'CONCURRENT')) as ordered,
        count(*) filter (where claimed_until > :now and cancelled_on is null
          and lane = 'CONCURRENT') as concurrent
      from outbox
      where claimed_until is not null and relayed_on is null""";

  private OutboxGaugeSql() {}
}
