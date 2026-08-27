package org.opentmf.outbox;

/**
 * Thrown by an {@link OutboxPublisher} when retrying is pointless (e.g. the destination
 * answered that the request itself is invalid): the relay skips the remaining attempts and
 * applies the publisher's {@link OutboxPublisher#onExhausted exhaustion outcome} immediately -
 * PARK or DROP - with this exception's message as {@code last_error}. Every other
 * {@link RuntimeException} means "retry".
 */
public class TerminalOutboxException extends RuntimeException {

  public TerminalOutboxException(String message) {
    super(message);
  }

  public TerminalOutboxException(String message, Throwable cause) {
    super(message, cause);
  }
}
