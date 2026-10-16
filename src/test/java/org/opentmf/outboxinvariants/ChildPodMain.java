package org.opentmf.outboxinvariants;

import java.time.Duration;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.opentmf.outboxinvariants.Receiver.Script;
import org.opentmf.outboxinvariants.Receiver.Step;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * The child pod's main: scripts its receiver from {@code -Dit.child.script}, starts the pod and
 * waits to be killed. The spec is {@code prefix;key=value;...} with {@code
 * lane=ORDERED|CONCURRENT}, {@code lease=ISO-8601}, {@code latency=ISO-8601}, {@code
 * steps=OK|HOLD|...}, {@code hookHolds}.
 */
public final class ChildPodMain {

  private ChildPodMain() {}

  public static void main(String[] args) throws InterruptedException {
    String[] spec = System.getProperty("it.child.script", "inv:").split(";");
    Script script = Script.concurrent();
    for (int i = 1; i < spec.length; i++) {
      String[] kv = spec[i].split("=", 2);
      switch (kv[0]) {
        case "lane" -> script = Lane.valueOf(kv[1]) == Lane.ORDERED ? Script.ordered() : script;
        case "lease" -> script.lease(Duration.parse(kv[1]));
        case "latency" -> script.latency(Duration.parse(kv[1]));
        case "steps" -> {
          String[] names = kv[1].split("\\|");
          Step[] steps = new Step[names.length];
          for (int s = 0; s < names.length; s++) {
            steps[s] = new Step(Receiver.Outcome.valueOf(names[s]), Duration.ZERO);
          }
          script.steps(steps);
        }
        case "hookHolds" -> script.hookHolds();
        default -> throw new IllegalArgumentException("unknown script key " + kv[0]);
      }
    }
    Receiver.script(spec[0], script);
    new SpringApplicationBuilder(PodApp.class).run(args);
    System.out.println("READY");
    System.out.flush();
    Thread.currentThread().join(); // until killed
  }
}
