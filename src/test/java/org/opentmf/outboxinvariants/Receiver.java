package org.opentmf.outboxinvariants;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxPublisher.ExhaustionOutcome;
import org.opentmf.outbox.OutboxPublisher.Lane;

/**
 * The programmable receiver behind every {@code inv:} destination: a {@link Script} per destination
 * prefix says the row's lane, key, lease, latency, budget, exhaustion and what each attempt does;
 * every attempt is recorded as a {@link Delivery} - which pod, which attempt, when it started and
 * ended, how it ended. The invariants are checked against these records and the table.
 *
 * <p>Shared by every pod of one JVM (static); a child-JVM pod prints its deliveries and the parent
 * adds them here ({@link #record}).
 */
public final class Receiver {

  private Receiver() {}

  /** What one attempt does. */
  public enum Outcome {
    /** Delivered after the script's latency. */
    OK,
    /** A retryable failure (an exception the relay books with backoff). */
    FAIL,
    /** A {@code TerminalOutboxException}: exhausted at once. */
    TERMINAL,
    /** Holds for {@link Step#hold()} after the latency, then delivers. */
    HANG,
    /** Holds until the scenario releases the receiver ({@link #release()}), then delivers. */
    HOLD,
    /**
     * Holds for {@link Step#hold()} IGNORING interrupts (a client that never honours one), then
     * delivers - the interrupt flag restored at the end.
     */
    DEAF,
    /** Throws a {@link Error} (a broken dependency, a stack overflow) - not an exception. */
    ERROR
  }

  /** One attempt's behaviour. */
  public record Step(Outcome outcome, Duration hold) {
    public static Step ok() {
      return new Step(Outcome.OK, Duration.ZERO);
    }

    public static Step fail() {
      return new Step(Outcome.FAIL, Duration.ZERO);
    }

    public static Step terminal() {
      return new Step(Outcome.TERMINAL, Duration.ZERO);
    }

    public static Step hang(Duration hold) {
      return new Step(Outcome.HANG, hold);
    }

    public static Step deaf(Duration hold) {
      return new Step(Outcome.DEAF, hold);
    }

    public static Step error() {
      return new Step(Outcome.ERROR, Duration.ZERO);
    }

    public static Step holdUntilReleased() {
      return new Step(Outcome.HOLD, Duration.ZERO);
    }
  }

  /** How rows of one destination prefix behave; fluent, every field optional. */
  public static final class Script {
    Lane lane = Lane.CONCURRENT;
    Function<OutboxEvent, String> key = OutboxEvent::getDestination;
    Duration lease;
    Duration minLatency = Duration.ZERO;
    Duration maxLatency = Duration.ZERO;
    List<Step> steps = List.of(Step.ok());
    Function<OutboxEvent, List<Step>> stepsPerRow;
    int maxAttempts;
    Duration backoff;
    ExhaustionOutcome exhausted = ExhaustionOutcome.PARK;
    boolean byDelivery;
    boolean hookThrows;
    boolean hookHolds;
    boolean listenerThrows;

    public static Script concurrent() {
      return new Script();
    }

    public static Script ordered() {
      Script script = new Script();
      script.lane = Lane.ORDERED;
      script.key = e -> null;
      return script;
    }

    public Script key(Function<OutboxEvent, String> keyOf) {
      this.key = keyOf;
      return this;
    }

    public Script unkeyed() {
      this.key = e -> null;
      return this;
    }

    public Script lease(Duration value) {
      this.lease = value;
      return this;
    }

    public Script latency(Duration fixed) {
      this.minLatency = fixed;
      this.maxLatency = fixed;
      return this;
    }

    public Script latency(Duration min, Duration max) {
      this.minLatency = min;
      this.maxLatency = max;
      return this;
    }

    /** Attempt n does step n (the last step repeats). */
    public Script steps(Step... perAttempt) {
      this.steps = List.of(perAttempt);
      return this;
    }

    /**
     * Step n is the row's n-th DELIVERY (any pod of this JVM), not its n-th booked attempt: a
     * redelivery after a lapsed lease is attempt 1 again, but its second delivery.
     */
    public Script byDelivery() {
      this.byDelivery = true;
      return this;
    }

    /** Per row: attempt n does step n of the row's own list. */
    public Script stepsPerRow(Function<OutboxEvent, List<Step>> perRow) {
      this.stepsPerRow = perRow;
      return this;
    }

    public Script budget(int attempts, Duration backoffEach) {
      this.maxAttempts = attempts;
      this.backoff = backoffEach;
      return this;
    }

    public Script exhausted(ExhaustionOutcome outcome) {
      this.exhausted = outcome;
      return this;
    }

    public Script hookThrows() {
      this.hookThrows = true;
      return this;
    }

    /** The booking hook waits for {@link Receiver#release()} - delivered, not yet booked. */
    public Script hookHolds() {
      this.hookHolds = true;
      return this;
    }

    public Script listenerThrows() {
      this.listenerThrows = true;
      return this;
    }

    Step step(OutboxEvent event, int attempt) {
      List<Step> mine = stepsPerRow != null ? stepsPerRow.apply(event) : steps;
      int n = byDelivery ? NTH.merge(event.getId(), 1, Integer::sum) : attempt;
      return mine.get(Math.min(n, mine.size()) - 1);
    }
  }

  /** One attempt as the receiver saw it. */
  public record Delivery(
      long rowId,
      String destination,
      String key,
      Lane lane,
      String pod,
      int attempt,
      Instant start,
      Instant end,
      Outcome outcome,
      boolean interrupted) {

    public Duration took() {
      return Duration.between(start, end);
    }

    /** Delivered: the receiver has the effect (a hang that ran out delivers too). */
    public boolean delivered() {
      return !interrupted
          && (outcome == Outcome.OK
              || outcome == Outcome.HANG
              || outcome == Outcome.HOLD
              || outcome == Outcome.DEAF);
    }
  }

  private static final Map<String, Script> SCRIPTS = new ConcurrentHashMap<>();
  private static final List<Delivery> DELIVERIES = new CopyOnWriteArrayList<>();
  private static final Map<Long, Instant> SENDING = new ConcurrentHashMap<>();
  private static final Map<Long, Integer> NTH = new ConcurrentHashMap<>();
  private static final Map<Long, Integer> SENDING_COUNT = new ConcurrentHashMap<>();

  /** Per row, the lease stamp its FIRST send carried - the claim that sent it, less its lease. */
  private static final Map<Long, OffsetDateTime> FIRST_STAMP = new ConcurrentHashMap<>();
  private static final Map<Long, Integer> LISTENED = new ConcurrentHashMap<>();
  private static volatile CountDownLatch released = new CountDownLatch(1);
  private static SplittableRandom random = new SplittableRandom(0);

  /** A new scenario: no scripts, no deliveries, the receiver held, the given seed. */
  public static void reset(long seed) {
    SCRIPTS.clear();
    DELIVERIES.clear();
    SENDING.clear();
    SENDING_COUNT.clear();
    FIRST_STAMP.clear();
    NTH.clear();
    LISTENED.clear();
    released.countDown();
    released = new CountDownLatch(1);
    synchronized (Receiver.class) {
      random = new SplittableRandom(seed);
    }
  }

  public static void script(String destinationPrefix, Script script) {
    SCRIPTS.put(destinationPrefix, script);
  }

  /** The script of the longest matching prefix. */
  static Script scriptFor(String destination) {
    return SCRIPTS.entrySet().stream()
        .filter(e -> destination.startsWith(e.getKey()))
        .max(Comparator.comparingInt(e -> e.getKey().length()))
        .map(Map.Entry::getValue)
        .orElseGet(Script::concurrent);
  }

  static Duration latency(Script script) {
    long min = script.minLatency.toNanos();
    long max = script.maxLatency.toNanos();
    if (max <= min) {
      return script.minLatency;
    }
    synchronized (Receiver.class) {
      return Duration.ofNanos(min + random.nextLong(max - min));
    }
  }

  /** A seeded draw for the scenarios' own randomness. */
  public static synchronized long nextLong(long bound) {
    return random.nextLong(bound);
  }

  public static synchronized double nextDouble() {
    return random.nextDouble();
  }

  /** The relayed listener fired for this row (once per firing). */
  static void listened(long rowId) {
    LISTENED.merge(rowId, 1, Integer::sum);
  }

  /** How often the relayed listener fired for each row. */
  public static Map<Long, Integer> listened() {
    return Map.copyOf(LISTENED);
  }

  /** Lets every HOLD step go. */
  public static void release() {
    released.countDown();
  }

  static boolean awaitRelease(Duration atMost) throws InterruptedException {
    return released.await(atMost.toNanos(), TimeUnit.NANOSECONDS);
  }

  /** The lease stamp a row's send carried (the first one counts). */
  static void stamped(long rowId, OffsetDateTime stamp) {
    if (stamp != null) {
      FIRST_STAMP.putIfAbsent(rowId, stamp);
    }
  }

  /** The stamp the row's first send carried, if a scripted publisher sent it. */
  static OffsetDateTime firstStamp(long rowId) {
    return FIRST_STAMP.get(rowId);
  }

  static void started(long rowId) {
    Instant now = Instant.now();
    SENDING.putIfAbsent(rowId, now);
    SENDING_COUNT.merge(rowId, 1, Integer::sum);
    if (Boolean.getBoolean("it.child")) {
      System.out.println("STARTED|" + rowId + "|" + now);
      System.out.flush();
    }
  }

  /** Records one attempt - from a pod of this JVM, or parsed from a child pod's output. */
  public static void record(Delivery delivery) {
    if (SENDING_COUNT.merge(delivery.rowId(), -1, Integer::sum) <= 0) {
      SENDING_COUNT.remove(delivery.rowId());
      SENDING.remove(delivery.rowId());
    }
    DELIVERIES.add(delivery);
    if (Boolean.getBoolean("it.child")) {
      System.out.println(
          String.join(
              "|",
              "DELIVERY",
              String.valueOf(delivery.rowId()),
              delivery.destination(),
              String.valueOf(delivery.key()),
              delivery.lane().name(),
              delivery.pod(),
              String.valueOf(delivery.attempt()),
              delivery.start().toString(),
              delivery.end().toString(),
              delivery.outcome().name(),
              String.valueOf(delivery.interrupted())));
      System.out.flush();
    }
  }

  /** A child pod's DELIVERY line, back as a record. */
  static Delivery parse(String line) {
    String[] f = line.split("\\|", -1);
    return new Delivery(
        Long.parseLong(f[1]),
        f[2],
        "null".equals(f[3]) ? null : f[3],
        Lane.valueOf(f[4]),
        f[5],
        Integer.parseInt(f[6]),
        Instant.parse(f[7]),
        Instant.parse(f[8]),
        Outcome.valueOf(f[9]),
        Boolean.parseBoolean(f[10]));
  }

  /** A child pod's STARTED line: the send is under way there. */
  static void startedElsewhere(String line) {
    String[] f = line.split("\\|", -1);
    SENDING.putIfAbsent(Long.parseLong(f[1]), Instant.parse(f[2]));
    SENDING_COUNT.merge(Long.parseLong(f[1]), 1, Integer::sum);
  }

  /** A send that never ended because its pod died: dropped from the in-progress set. */
  static void forget(long rowId) {
    SENDING.remove(rowId);
    SENDING_COUNT.remove(rowId);
  }

  public static List<Delivery> deliveries() {
    return new ArrayList<>(DELIVERIES);
  }

  /** Attempts started and not ended - a hang still running, a send the relay interrupted. */
  public static Map<Long, Instant> sending() {
    return Map.copyOf(SENDING);
  }
}
