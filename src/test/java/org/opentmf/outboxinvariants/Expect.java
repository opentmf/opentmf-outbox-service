package org.opentmf.outboxinvariants;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * What a scenario lets the invariants allow - every allowance names a documented behaviour, never a
 * convenience. The default allows nothing.
 */
public final class Expect {

  /** Rows whose extra deliveries the scenario explains (a documented at-least-once window). */
  final Set<Long> mayDuplicate = new HashSet<>();

  /** Every row may be delivered more than once (a transport's documented at-least-once). */
  boolean atLeastOnce;

  /** The attempts/relayed counters can be compared exactly (no unpark resets attempts). */
  boolean exactCounters = true;

  /** ORDERED first attempts of never-failed, never-held rows go in id order on each pod. */
  boolean orderedIdOrder = true;

  /** End with the progress probe on every live pod (I5). */
  boolean probe = true;

  /** A bound on any row's wait while a slot was free; null = one sweep plus the row's lease. */
  Duration starvationBound;

  /** Rows were pruned: the counters cannot be compared with the table. */
  boolean pruned;

  /** No starvation check: the receiver sees no send start (a broker's consumer). */
  boolean skipStarvation;

  /** Rows not expected to be relayed at the end (a hold beyond the scenario, a dead receiver). */
  final Set<Long> mayStayOpen = new HashSet<>();

  public static Expect strict() {
    return new Expect();
  }

  public Expect mayDuplicate(long rowId) {
    mayDuplicate.add(rowId);
    return this;
  }

  public Expect atLeastOnce() {
    atLeastOnce = true;
    return this;
  }

  public Expect attemptsReset() {
    exactCounters = false;
    return this;
  }

  public Expect noOrderedIdOrder() {
    orderedIdOrder = false;
    return this;
  }

  public Expect noProbe() {
    probe = false;
    return this;
  }

  public Expect starvationBound(Duration bound) {
    starvationBound = bound;
    return this;
  }

  public Expect pruned() {
    pruned = true;
    return this;
  }

  public Expect skipStarvation() {
    skipStarvation = true;
    return this;
  }

  public Expect mayStayOpen(long rowId) {
    mayStayOpen.add(rowId);
    return this;
  }
}
