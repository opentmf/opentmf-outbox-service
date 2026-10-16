package org.opentmf.outboxinvariants;

import java.time.Duration;
import java.time.Instant;
import org.opentmf.outbox.OutboxBooking;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.TerminalOutboxException;
import org.opentmf.outboxinvariants.Receiver.Delivery;
import org.opentmf.outboxinvariants.Receiver.Script;
import org.opentmf.outboxinvariants.Receiver.Step;

/**
 * The publisher of every {@code inv:} destination: does what the {@link Receiver}'s script says,
 * and records each attempt. One per pod; the pod's name goes into every record.
 */
public class ScriptedPublisher implements OutboxPublisher {

  /** The destination prefix this publisher takes. */
  public static final String PREFIX = "inv:";

  private final String pod;

  public ScriptedPublisher(String pod) {
    this.pod = pod;
  }

  private static Script script(OutboxEvent event) {
    return Receiver.scriptFor(event.getDestination());
  }

  @Override
  public boolean supports(OutboxEvent event) {
    return event.getDestination().startsWith(PREFIX);
  }

  @Override
  public Lane lane(OutboxEvent event) {
    return script(event).lane;
  }

  @Override
  public String orderingKey(OutboxEvent event) {
    return script(event).key.apply(event);
  }

  @Override
  public Duration lease(OutboxEvent event) {
    return script(event).lease;
  }

  @Override
  public int maxAttempts(OutboxEvent event) {
    return script(event).maxAttempts;
  }

  @Override
  public Duration backoff(OutboxEvent event, int attempt) {
    return script(event).backoff;
  }

  @Override
  public ExhaustionOutcome onExhausted(OutboxEvent event) {
    return script(event).exhausted;
  }

  @Override
  public void publish(OutboxEvent event) {
    Script script = script(event);
    int attempt = event.getAttempts() + 1;
    Step step = script.step(event, attempt);
    Instant start = Instant.now();
    Receiver.started(event.getId());
    Receiver.stamped(event.getId(), event.getClaimedUntil()); // the claim's stamp, as handed over
    boolean interrupted = false;
    try {
      pause(Receiver.latency(script, event.getId(), attempt));
      switch (step.outcome()) {
        case FAIL -> throw new IllegalStateException("scripted failure, attempt " + attempt);
        case TERMINAL -> throw new TerminalOutboxException("scripted refusal, attempt " + attempt);
        case ERROR -> throw new StackOverflowError("scripted Error, attempt " + attempt);
        case HANG -> pause(step.hold());
        case DEAF -> deaf(step.hold());
        case HOLD -> {
          if (!Receiver.awaitRelease(Duration.ofMinutes(10))) {
            throw new IllegalStateException("the scenario never released the receiver");
          }
        }
        case OK -> {
          // delivered after the latency
        }
      }
    } catch (InterruptedException ex) {
      interrupted = true;
      Thread.currentThread().interrupt();
      throw new IllegalStateException("send interrupted", ex);
    } finally {
      Receiver.recordDelivery(
          new Delivery(
              event.getId(),
              event.getDestination(),
              script.key.apply(event),
              event.getLane() != null ? event.getLane() : Lane.ORDERED,
              pod,
              attempt,
              start,
              Instant.now(),
              step.outcome(),
              interrupted));
    }
  }

  @Override
  public void onBooked(OutboxEvent event, OutboxBooking booking) {
    Script script = script(event);
    if (script.hookHolds) { // delivered, booking open: the window a killed pod dies in
      if (Boolean.getBoolean("it.child")) {
        System.out.println("BOOKING|" + event.getId());
        System.out.flush();
      }
      try {
        Receiver.awaitRelease(Duration.ofMinutes(10));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
    if (script.hookThrows) {
      throw new IllegalStateException("scripted booking hook failure");
    }
  }

  /** Sleeps through every interrupt, then restores the flag - as a deaf blocking call would. */
  private static void deaf(Duration duration) {
    Waits.deafly(duration);
  }

  private static void pause(Duration duration) throws InterruptedException {
    Waits.interruptibly(duration);
  }
}
