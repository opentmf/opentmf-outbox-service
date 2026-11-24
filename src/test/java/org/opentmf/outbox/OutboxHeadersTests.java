package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The idempotency key (OUTBOX-IDEMPOTENCY-KEY-REPEATS-AFTER-RESTORE-1): {@code
 * <service>:outbox:<id>:<key_token>} for a row with a key token, exactly the 1.11.0 key for a row
 * without one - and what a rolling start from 1.11.0 does with each.
 */
class OutboxHeadersTests {

  private static final UUID TOKEN = UUID.fromString("0b7c1a52-9d3e-4f6a-8c21-5e4d3b2a1f00");

  private static OutboxEvent row(long id, UUID token) {
    OutboxEvent event = new OutboxEvent();
    event.setId(id);
    event.setKeyToken(token);
    return event;
  }

  @Test
  void aRowWithAKeyToken_isKeyedByItsIdAndItsToken() {
    assertThat(OutboxHeaders.idempotencyKey("svc", row(42, TOKEN)))
        .isEqualTo("svc:outbox:42:0b7c1a52-9d3e-4f6a-8c21-5e4d3b2a1f00");
  }

  /** The same id with another token - a reused id after a restore - is another key. */
  @Test
  void aReusedId_withANewToken_isANewKey() {
    assertThat(OutboxHeaders.idempotencyKey("svc", row(42, UUID.randomUUID())))
        .isNotEqualTo(OutboxHeaders.idempotencyKey("svc", row(42, TOKEN)));
  }

  /** A row booked before 1.12.0 has no token: its key is the one it was first sent with. */
  @Test
  void aRowWithoutAKeyToken_keepsThe1110Key() {
    assertThat(OutboxHeaders.idempotencyKey("svc", row(42, null))).isEqualTo("svc:outbox:42");
  }

  /** The deprecated method builds the 1.11.0 key, unchanged, for a consumer's own publisher. */
  @Test
  @SuppressWarnings("deprecation") // the deprecated method's own contract
  void theDeprecatedMethod_buildsThe1110KeyUnchanged() {
    assertThat(OutboxHeaders.idempotencyKey("svc", 42L)).isEqualTo("svc:outbox:42");
  }

  /**
   * A rolling start, 1.11.0 and this version side by side. A 1.11.0 pod builds every key from the
   * id alone (1.11.0's publishers called the method now deprecated) and ignores the column.
   *
   * <ul>
   *   <li>A row an OLD pod booked has no token: both versions send it under the same key - a
   *       re-send across versions is recognised.
   *   <li>A row a NEW pod booked has a token. Sent by an old pod it carries the 1.11.0 key, sent by
   *       a new pod the token key: if the same row is sent by both (a retry or a lapsed lease that
   *       crosses versions), the consumer sees two keys and keeps the event TWICE - one duplicate,
   *       for that row, only while both versions relay. Roll at a low backlog.
   * </ul>
   */
  @Test
  @SuppressWarnings("deprecation") // stands in for a 1.11.0 pod's publisher
  void aRollingStart_aRowBookedByANewPodAndSentByAnOldOne_carriesTheOldKey() {
    OutboxEvent bookedByAnOldPod = row(41, null);
    assertThat(OutboxHeaders.idempotencyKey("svc", bookedByAnOldPod))
        .as("an old pod's row: both versions agree")
        .isEqualTo(OutboxHeaders.idempotencyKey("svc", bookedByAnOldPod.getId()));

    OutboxEvent bookedByANewPod = row(42, TOKEN);
    String sentByAnOldPod = OutboxHeaders.idempotencyKey("svc", bookedByANewPod.getId());
    String sentByANewPod = OutboxHeaders.idempotencyKey("svc", bookedByANewPod);
    assertThat(sentByAnOldPod).isEqualTo("svc:outbox:42");
    assertThat(sentByANewPod)
        .as("a new pod's row sent by both versions: two keys, one duplicate")
        .isNotEqualTo(sentByAnOldPod);
  }
}
