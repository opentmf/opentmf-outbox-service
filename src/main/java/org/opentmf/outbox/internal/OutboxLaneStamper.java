package org.opentmf.outbox.internal;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import lombok.extern.slf4j.Slf4j;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.opentmf.outbox.OutboxWriter;

/**
 * Stamps a row's LANE and ORDERING KEY at append (1.3.0), through the same router the relay uses,
 * so each lane can claim over its own index and neither ever reads through the other's backlog.
 * Frozen from then on, like the headers. Public only so {@link OutboxWriter} (another package)
 * can call it - the seal keeps consumers out of this package.
 *
 * <p>An append never fails the business transaction on this account: a row no publisher
 * supports, or whose publisher throws while naming its lane or key, is stored with no lane (it
 * rides ORDERED, where the relay books the routing failure exactly as before) and a WARN.
 */
@Slf4j
public final class OutboxLaneStamper {

  /** The column width; a longer key is stored as {@code sha256:<hex>}. */
  static final int ORDERING_KEY_MAX_LENGTH = 255;

  private final OutboxPublisherRouter router;

  OutboxLaneStamper(OutboxPublisherRouter router) {
    this.router = router;
  }

  /** Sets {@code lane} and {@code orderingKey} on a row about to be inserted. */
  public void stamp(OutboxEvent event) {
    try {
      OutboxPublisher publisher = router.resolve(event);
      Lane lane = publisher.lane(event) == Lane.CONCURRENT ? Lane.CONCURRENT : Lane.ORDERED;
      event.setLane(lane);
      event.setOrderingKey(lane == Lane.CONCURRENT ? fit(publisher.orderingKey(event)) : null);
    } catch (RuntimeException ex) {
      event.setLane(null);
      event.setOrderingKey(null);
      log.warn(
          "Outbox row to {} appended without a lane - it rides ORDERED and the relay books the"
              + " routing failure: {}",
          event.getDestination(),
          ex.toString());
    }
  }

  static String fit(String key) {
    if (key == null || key.isBlank()) {
      return null; // no key: the row is independent
    }
    if (key.length() <= ORDERING_KEY_MAX_LENGTH) {
      return key;
    }
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8));
      return "sha256:" + HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException ex) {
      throw new IllegalStateException("SHA-256 is a mandatory JDK algorithm", ex);
    }
  }
}
