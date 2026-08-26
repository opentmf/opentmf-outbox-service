package org.opentmf.outbox;

import java.util.Locale;

/**
 * The ops list state vocabulary - a CLOSED set whose closure IS the contract (the outbox
 * defines exactly these three DERIVED states; a fourth would break the no-status-column design,
 * so an unknown value fails loudly at the binding, never tolerated). Filtering semantics:
 *
 * <ul>
 *   <li>{@code pending} - {@code relayed_on is null} (parked INCLUDED: parked is a sub-state)
 *   <li>{@code parked} - pending AND {@code attempts >= max-attempts}
 *   <li>{@code relayed} - {@code relayed_on is not null}
 * </ul>
 */
public enum OutboxStateFilter {
  PENDING,
  PARKED,
  RELAYED;

  /** Case-insensitive wire parse; unknown = loud {@link IllegalArgumentException} (400). */
  public static OutboxStateFilter fromWire(String wire) {
    try {
      return valueOf(wire.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException ignored) {
      throw new IllegalArgumentException(
          "Unknown outbox state '%s' - the closed set is pending|parked|relayed".formatted(wire));
    }
  }
}
