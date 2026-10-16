package org.opentmf.outboxinvariants;

import java.util.ArrayList;
import java.util.List;

/** What one scenario found: every violation, the figures it measured, notes. */
public final class Findings {

  final String scenario;
  final long seed;
  final List<String> items = new ArrayList<>();
  final List<String> figures = new ArrayList<>();
  final List<String> notes = new ArrayList<>();

  Findings(String scenario, long seed) {
    this.scenario = scenario;
    this.seed = seed;
  }

  /** One violation of invariant {@code id} (I1..I6, Q for quiescence, S for the scenario's own). */
  public void add(String id, String text) {
    items.add(id + ": " + text);
  }

  public void figure(String text) {
    figures.add(text);
  }

  public boolean isEmpty() {
    return items.isEmpty();
  }

  public List<String> items() {
    return List.copyOf(items);
  }

  /** Fails with EVERY finding and the seed - never the first only. */
  public void assertNone() {
    if (!items.isEmpty()) {
      throw new AssertionError(this.toString());
    }
  }

  @Override
  public String toString() {
    StringBuilder text =
        new StringBuilder(
            "INVARIANTS: scenario %s seed %d - %s%n"
                .formatted(
                    scenario,
                    seed,
                    items.isEmpty() ? "all invariants hold" : items.size() + " finding(s)"));
    items.forEach(item -> text.append("  FINDING ").append(item).append('\n'));
    figures.forEach(f -> text.append("  figure  ").append(f).append('\n'));
    notes.forEach(n -> text.append("  note    ").append(n).append('\n'));
    return text.toString();
  }
}
