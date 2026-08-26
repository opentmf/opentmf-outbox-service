package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;

/** The CLOSED state vocabulary: three states, loud unknown. */
class OutboxStateFilterTests {

  @Test
  void wireParse_isCaseInsensitive() {
    assertThat(OutboxStateFilter.fromWire("parked")).isEqualTo(OutboxStateFilter.PARKED);
    assertThat(OutboxStateFilter.fromWire("Pending")).isEqualTo(OutboxStateFilter.PENDING);
    assertThat(OutboxStateFilter.fromWire("RELAYED")).isEqualTo(OutboxStateFilter.RELAYED);
  }

  @Test
  void unknownState_failsLoud_theClosureIsTheContract() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> OutboxStateFilter.fromWire("dead-lettered"))
        .withMessageContaining("pending|parked|relayed");
  }
}
