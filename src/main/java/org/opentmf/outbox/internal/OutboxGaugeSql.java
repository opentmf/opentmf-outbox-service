package org.opentmf.outbox.internal;

/**
 * The gauge queries, as native SQL constants - run by {@link OutboxEventRepository} and pinned
 * by {@code OutboxClaimPlanIT} with the very same text. None reads the relayed part of the
 * table; each is served by a partial index whose predicate it repeats:
 *
 * <ul>
 *   <li>{@link #OPEN_NOT_PARKED} - {@code ix_outbox_pending} (index-only count);
 *   <li>{@link #PARKED} - {@code ix_outbox_parked};
 *   <li>{@link #OPEN_SINCE} - {@code ix_outbox_open_since}, its first entry: the instant the
 *       oldest open row became deliverable ({@code greatest(created_on, release_at)}) - a held
 *       row's instant lies in the future, so it lags only once its hold has passed;
 *   <li>{@link #IN_FLIGHT} - {@code ix_outbox_claimed_until}: its WHERE is exactly that index's
 *       predicate and the time test sits in the aggregate's FILTER - a {@code claimed_until >
 *       :now} in the WHERE leaves a generic plan blind to how few rows it selects, and it then
 *       picked {@code ix_outbox_open_since} and filtered every open row.
 * </ul>
 *
 * <p>Their cost grows with the OPEN rows (pending, parked, in flight), never with the relayed
 * history. They run on the metrics refresh schedule, never on a scrape.
 */
final class OutboxGaugeSql {

  static final String OPEN_NOT_PARKED =
      """
      select count(*) from outbox
      where relayed_on is null and cancelled_on is null and parked_on is null""";

  static final String PARKED =
      """
      select count(*) from outbox
      where parked_on is not null and relayed_on is null and cancelled_on is null""";

  static final String OPEN_SINCE =
      """
      select min(greatest(created_on, coalesce(release_at, created_on))) from outbox
      where relayed_on is null and cancelled_on is null""";

  static final String IN_FLIGHT =
      """
      select count(*) filter (where claimed_until > :now and cancelled_on is null) from outbox
      where claimed_until is not null and relayed_on is null""";

  private OutboxGaugeSql() {}
}
