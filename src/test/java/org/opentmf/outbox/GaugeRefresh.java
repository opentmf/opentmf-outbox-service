package org.opentmf.outbox;

import java.lang.reflect.Method;
import org.springframework.context.ApplicationContext;

/**
 * Test support: refreshes the outbox gauges' snapshot NOW, on the calling thread. An IT that
 * asserts a gauge turns the scheduled refresh off ({@code opentmf.outbox.metrics-refresh=1h} -
 * one refresh at start, none after) and refreshes at the points where it reads a gauge, so the
 * refresher never takes a database connection behind the test's back - a test that samples the
 * pool for zero checked-out connections would race it.
 */
final class GaugeRefresh {

  private GaugeRefresh() {}

  /** Runs one refresh of the library's (package-private) metrics bean. */
  static void now(ApplicationContext context) {
    Object metrics = context.getBean("outboxMetrics");
    try {
      Method refresh = metrics.getClass().getDeclaredMethod("refresh");
      refresh.setAccessible(true);
      refresh.invoke(metrics);
    } catch (ReflectiveOperationException ex) {
      throw new IllegalStateException(ex);
    }
  }
}
