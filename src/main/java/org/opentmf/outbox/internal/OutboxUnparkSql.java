package org.opentmf.outbox.internal;

/**
 * The unpark by FILTER, as native SQL constants - run by {@link OutboxEventRepository} and pinned
 * by {@code OutboxClaimPlanIT} with the very same text. One BATCH per statement: the parked rows
 * of ONE destination within a parked-on range (the caller passes {@code -infinity} /
 * {@code infinity} for an open end - never an {@code OR} on a parameter), oldest parked first,
 * found through {@code ix_outbox_parked (destination, parked_on, id)}, each left exactly as the
 * single unpark leaves it: {@code parked_on} cleared, {@code attempts} reset, due now,
 * {@code last_error} kept. The rows are locked with a WAITING {@code FOR UPDATE} - the same guard
 * as the single unpark: an ops action or booking holding a row is waited for, and the row is
 * re-checked as that transaction left it (cancelled or already unparked meanwhile = not taken).
 * {@link #BY_REFERENCE} narrows to one {@code reference} as well.
 */
final class OutboxUnparkSql {

  private static final String SET =
      """
      update outbox set parked_on = null, attempts = 0, next_attempt_on = :now
      where id = any (array(
        select p.id from outbox p
        where p.parked_on is not null and p.relayed_on is null and p.cancelled_on is null
          and p.destination = :destination
          and p.parked_on >= :parkedFrom and p.parked_on < :parkedTo
      """;

  private static final String BATCH =
      """
        order by p.parked_on, p.id limit :limit
        for update))""";

  /** One batch of one destination's parked rows within the range. */
  static final String BY_DESTINATION = SET + BATCH;

  /** The same, narrowed to one {@code reference}. */
  static final String BY_REFERENCE = SET + "    and p.reference = :reference\n" + BATCH;

  private OutboxUnparkSql() {}
}
