package org.opentmf.outbox;

/**
 * What one bounded unpark-by-filter call did: the parked rows it returned to delivery, and
 * whether matching rows remain because the call stopped on its time budget - the caller then
 * simply calls again.
 *
 * @param unparked rows unparked by this call
 * @param moreToUnpark whether the call stopped on its budget with matching rows left
 */
public record OutboxUnparkResult(long unparked, boolean moreToUnpark) {}
