package org.opentmf.outboxinvariants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A pod in its OWN JVM - the one way to die for real: {@link #kill()} is SIGKILL, so nothing of the
 * pod's shutdown runs, its open transactions roll back and its leases simply stop. The child runs
 * {@link ChildPodMain} on this JVM's class path and prints every attempt; the parent records them
 * in the {@link Receiver}.
 */
public final class ChildPod {

  final String name;
  private final Process process;
  private final CountDownLatch ready = new CountDownLatch(1);
  private final Set<Long> booking = ConcurrentHashMap.newKeySet();
  private final Set<Long> started = ConcurrentHashMap.newKeySet();

  private ChildPod(String name, Process process) {
    this.name = name;
    this.process = process;
  }

  /**
   * Starts a child pod.
   *
   * @param script the child's {@link ChildPodMain} script spec
   * @param properties the pod's Spring properties
   */
  static ChildPod start(String name, String script, Map<String, String> properties) {
    String java = ProcessHandle.current().info().command().orElse("java");
    List<String> command = new ArrayList<>();
    command.add(java);
    command.add("-Dit.child=true");
    command.add("-Dit.child.script=" + script);
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ChildPodMain.class.getName());
    properties.forEach((k, v) -> command.add("--" + k + "=" + v));
    try {
      Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
      ChildPod child = new ChildPod(name, process);
      Thread reader = new Thread(child::read, "child-pod-" + name);
      reader.setDaemon(true);
      reader.start();
      if (!child.ready.await(90, TimeUnit.SECONDS)) {
        child.kill();
        throw new IllegalStateException("child pod " + name + " did not start in 90 s");
      }
      return child;
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(ex);
    }
  }

  private void read() {
    try (BufferedReader in =
        new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      for (String line = in.readLine(); line != null; line = in.readLine()) {
        if (line.startsWith("DELIVERY|")) {
          Receiver.record(Receiver.parse(line));
        } else if (line.startsWith("STARTED|")) {
          Receiver.startedElsewhere(line);
          started.add(Long.parseLong(line.split("\\|")[1]));
        } else if (line.startsWith("BOOKING|")) {
          booking.add(Long.parseLong(line.split("\\|")[1]));
        } else if (line.equals("READY")) {
          ready.countDown();
        } else if (line.contains("ERROR") || line.contains("Exception")) {
          System.out.println("[" + name + "] " + line);
        }
      }
    } catch (IOException ex) {
      // the child died
    }
  }

  /** The rows whose booking this child holds open (delivered, not booked). */
  public Set<Long> booking() {
    return Set.copyOf(booking);
  }

  /** The rows whose send this child began. */
  public Set<Long> started() {
    return Set.copyOf(started);
  }

  public boolean alive() {
    return process.isAlive();
  }

  /** SIGKILL: no shutdown hook, no grace, no booking. */
  public void kill() {
    process.destroyForcibly();
    try {
      process.waitFor(30, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    started.forEach(Receiver::forget);
  }
}
