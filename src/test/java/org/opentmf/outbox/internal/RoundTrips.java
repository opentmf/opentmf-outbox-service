package org.opentmf.outbox.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DelegatingDataSource;

/**
 * A DataSource that counts the application's JDBC ROUND TRIPS (OutboxRelayThroughputIT, 1.11.1):
 * every statement executed, every commit, rollback and isolation change - a figure no other load on
 * the machine can move. Connection and statement proxies over the pool's own; nothing else changes.
 */
final class RoundTrips extends DelegatingDataSource {

  private static final AtomicLong COUNT = new AtomicLong();

  private static final Set<String> CONNECTION_TRIPS =
      Set.of("commit", "rollback", "setTransactionIsolation");
  private static final Set<String> STATEMENT_TRIPS =
      Set.of("execute", "executeQuery", "executeUpdate", "executeLargeUpdate", "executeBatch");

  RoundTrips(DataSource pool) {
    super(pool);
  }

  /** The round trips counted since the last {@link #reset}. */
  static long count() {
    return COUNT.get();
  }

  static void reset() {
    COUNT.set(0);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return counted(super.getConnection());
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return counted(super.getConnection(username, password));
  }

  private static Connection counted(Connection connection) {
    return (Connection)
        Proxy.newProxyInstance(
            RoundTrips.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              if (CONNECTION_TRIPS.contains(method.getName())) {
                COUNT.incrementAndGet();
              }
              Object result = invoke(connection, method, args);
              return result instanceof Statement statement
                  ? counted(statement, method.getReturnType())
                  : result;
            });
  }

  private static Object counted(Statement statement, Class<?> type) {
    return Proxy.newProxyInstance(
        RoundTrips.class.getClassLoader(),
        new Class<?>[] {type},
        (proxy, method, args) -> {
          if (STATEMENT_TRIPS.contains(method.getName())) {
            COUNT.incrementAndGet();
          }
          return invoke(statement, method, args);
        });
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException ex) {
      throw ex.getCause();
    }
  }
}
