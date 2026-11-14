package org.opentmf.outboxshutdown;

import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxPublisher.Lane;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;

/**
 * The consumer of {@link OutboxShutdownIT}: the library auto-configured over a {@link
 * RecordingPool} - the application's Hikari pool, recording every connection borrowed and held
 * against the moment its close begins - and a publisher for {@code stuck:} rows whose send blocks
 * until it is interrupted, as a Kafka send blocks on a dead broker, then takes its time before it
 * fails. Its own package: no other IT's context scans it.
 */
@SpringBootApplication
public class ShutdownApp {

  /** Signalled when a {@code stuck:} send is under way. */
  static volatile CountDownLatch sending = new CountDownLatch(1);

  @Bean
  RecordingPool dataSource(Environment environment) {
    RecordingPool pool = new RecordingPool();
    pool.setJdbcUrl(environment.getRequiredProperty("it.jdbc-url"));
    pool.setDriverClassName("org.testcontainers.jdbc.ContainerDatabaseDriver");
    pool.setMaximumPoolSize(12);
    return pool;
  }

  @Bean
  @Order(Ordered.HIGHEST_PRECEDENCE)
  OutboxPublisher stuckPublisher(Environment environment) {
    Lane lane = Lane.valueOf(environment.getProperty("it.lane", "CONCURRENT"));
    return new OutboxPublisher() {
      @Override
      public boolean supports(OutboxEvent event) {
        return event.getDestination().startsWith("stuck:");
      }

      @Override
      public Lane lane(OutboxEvent event) {
        return lane;
      }

      @Override
      public void publish(OutboxEvent event) {
        sending.countDown();
        try {
          new CountDownLatch(1).await(); // a send on a dead broker: until interrupted
        } catch (InterruptedException ex) {
          Hold.uninterruptibly(Duration.ofMillis(500)); // the producer's own way out
          Thread.currentThread().interrupt();
          throw new IllegalStateException("interrupted on a dead broker", ex);
        }
      }
    };
  }

  /** A call that takes its time and ignores interrupts, as a socket read does. */
  static final class Hold {

    private Hold() {}

    static void uninterruptibly(Duration time) {
      CountDownLatch never = new CountDownLatch(1);
      long end = System.nanoTime() + time.toNanos();
      for (long left = time.toNanos(); left > 0; left = end - System.nanoTime()) {
        try {
          if (never.await(left, TimeUnit.NANOSECONDS)) {
            return;
          }
        } catch (InterruptedException ignored) {
          // as a blocking read ignores it
        }
      }
    }
  }

  /**
   * The application's pool, recording: connections held when its close begins, connections borrowed
   * (or asked for) after it began, and by which threads. The FIRST borrow of a thread whose name
   * starts with {@link #slowFor} takes {@link #SLOW} - one slow round trip, well inside the stop's
   * join - so the stop finds that thread mid-way; its later borrows are not slowed (a gauge refresh
   * borrows once per query, four times: slowing each would outlast any join).
   */
  public static class RecordingPool extends HikariDataSource {

    static final Duration SLOW = Duration.ofSeconds(1);

    final AtomicInteger held = new AtomicInteger();
    volatile int heldAtClose = -1;
    final List<String> borrowedAfterClose = new CopyOnWriteArrayList<>();
    volatile boolean closing;
    volatile String slowFor;
    final CountDownLatch slowEntered = new CountDownLatch(1);
    private final AtomicBoolean slowed = new AtomicBoolean();

    @Override
    public Connection getConnection() throws SQLException {
      String thread = Thread.currentThread().getName();
      String slow = slowFor;
      if (slow != null && thread.startsWith(slow) && slowed.compareAndSet(false, true)) {
        slowEntered.countDown();
        Hold.uninterruptibly(SLOW);
      }
      if (closing) {
        borrowedAfterClose.add(thread);
      }
      Connection connection = super.getConnection();
      held.incrementAndGet();
      return (Connection)
          Proxy.newProxyInstance(
              RecordingPool.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                if ("close".equals(method.getName())) {
                  held.decrementAndGet();
                }
                try {
                  return method.invoke(connection, args);
                } catch (InvocationTargetException ex) {
                  throw ex.getCause();
                }
              });
    }

    @Override
    public void close() {
      heldAtClose = held.get();
      closing = true;
      super.close();
    }
  }
}
