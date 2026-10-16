package org.opentmf.outboxinvariants;

import io.micrometer.core.instrument.MeterRegistry;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;
import org.opentmf.outbox.OutboxAppend;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxMaintenanceService;
import org.opentmf.outbox.OutboxWriter;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.transaction.support.TransactionTemplate;

/** One running pod of a scenario: its context and the beans a scenario drives. */
public final class Pod implements AutoCloseable {

  final String name;
  final ConfigurableApplicationContext context;
  final Instant startedAt = Instant.now();

  /** The registry, held past the context's close: the counters of a stopped pod still count. */
  final MeterRegistry registryHandle;

  volatile Instant stoppedAt;

  /** This pod's CONCURRENT slots ({@code concurrent.max-in-flight}). */
  int slots = 8;

  Pod(String name, ConfigurableApplicationContext context) {
    this.name = name;
    this.context = context;
    this.registryHandle = context.getBean(MeterRegistry.class);
  }

  public String name() {
    return name;
  }

  public boolean alive() {
    return context.isActive();
  }

  public OutboxWriter writer() {
    return context.getBean(OutboxWriter.class);
  }

  public OutboxMaintenanceService maintenance() {
    return context.getBean(OutboxMaintenanceService.class);
  }

  public MeterRegistry registry() {
    return context.getBean(MeterRegistry.class);
  }

  public TransactionTemplate tx() {
    return context.getBean(TransactionTemplate.class);
  }

  public <T> T bean(Class<T> type) {
    return context.getBean(type);
  }

  /** One business transaction appending one row; returns its id. */
  public long append(OutboxAppend row) {
    OutboxEvent saved = tx().execute(status -> writer().append(row));
    return saved.getId();
  }

  /** One row to {@code destination} for {@code aggregateId}. */
  public long append(String destination, String aggregateId) {
    return append(
        OutboxAppend.of("it", aggregateId, "it.event.v1", destination, Map.of("a", aggregateId)));
  }

  /** One row held until {@code releaseAt}. */
  public long append(String destination, String aggregateId, OffsetDateTime releaseAt) {
    return append(
        OutboxAppend.of("it", aggregateId, "it.event.v1", destination, Map.of("a", aggregateId))
            .withReleaseAt(releaseAt));
  }

  /** Refreshes the gauges now, on this thread (the library's package-private bean, by name). */
  public void refreshGauges() {
    Object metrics = context.getBean("outboxMetrics");
    try {
      Method refresh = metrics.getClass().getDeclaredMethod("refresh");
      refresh.setAccessible(true);
      refresh.invoke(metrics);
    } catch (ReflectiveOperationException ex) {
      throw new IllegalStateException(ex);
    }
  }

  @Override
  public void close() {
    if (context.isActive()) {
      context.close();
      stoppedAt = Instant.now();
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
