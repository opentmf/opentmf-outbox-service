package org.opentmf.outbox.internal;

/**
 * The retention prune, as native SQL constants - run by {@link OutboxEventRepository} and pinned
 * by {@code OutboxClaimPlanIT} with the very same text. One BATCH per statement: the oldest
 * expired ids, found through {@code ix_outbox_relayed_on} / {@code ix_outbox_cancelled_on}
 * ({@code FOR UPDATE SKIP LOCKED}: a row an ops action or a booking holds is left for the next
 * batch), collected into one array, then deleted by primary key with {@code id = any(...)} - no
 * join the planner could turn into a full scan, and no entity ever loaded. Parked rows have
 * neither stamp, so they are structurally never pruned.
 */
final class OutboxPruneSql {

  static final String RELAYED =
      """
      delete from outbox
      where id = any (array(
        select r.id from outbox r
        where r.relayed_on < :cutoff
        order by r.relayed_on limit :limit
        for update skip locked))""";

  static final String CANCELLED =
      """
      delete from outbox
      where id = any (array(
        select c.id from outbox c
        where c.cancelled_on < :cutoff
        order by c.cancelled_on limit :limit
        for update skip locked))""";

  private OutboxPruneSql() {}
}
