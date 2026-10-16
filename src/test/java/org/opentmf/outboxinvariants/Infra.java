package org.opentmf.outboxinvariants;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

/**
 * The scenarios' shared infrastructure, started once per JVM: one Postgres, reached by the pods
 * through a Toxiproxy proxy (latency, outage) and by the checker directly. Ryuk removes the
 * containers when the JVM ends.
 */
final class Infra {

  private static final Network NETWORK = Network.newNetwork();
  private static final PostgreSQLContainer POSTGRES =
      new PostgreSQLContainer("postgres:18.1-alpine3.22")
          .withNetwork(NETWORK)
          .withNetworkAliases("postgres")
          .withCommand("postgres", "-c", "max_connections=500");
  private static final ToxiproxyContainer TOXIPROXY =
      new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.12.0").withNetwork(NETWORK);
  private static Proxy proxy;
  private static boolean migrated;
  private static KafkaContainer kafka;

  private Infra() {}

  static synchronized void start() {
    if (proxy != null) {
      return;
    }
    POSTGRES.start();
    TOXIPROXY.start();
    try {
      proxy =
          new ToxiproxyClient(TOXIPROXY.getHost(), TOXIPROXY.getControlPort())
              .createProxy("postgres", "0.0.0.0:8666", "postgres:5432");
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  /** The pods' JDBC URL: through the proxy. */
  static String podJdbcUrl() {
    start();
    return "jdbc:postgresql://%s:%d/%s"
        .formatted(TOXIPROXY.getHost(), TOXIPROXY.getMappedPort(8666), POSTGRES.getDatabaseName());
  }

  /** The checker's JDBC URL: past the proxy, so an outage never blinds it. */
  static String directJdbcUrl() {
    start();
    return POSTGRES.getJdbcUrl();
  }

  static String username() {
    return POSTGRES.getUsername();
  }

  static String password() {
    return POSTGRES.getPassword();
  }

  static Connection connect() throws SQLException {
    return DriverManager.getConnection(directJdbcUrl(), username(), password());
  }

  static Proxy proxy() {
    start();
    return proxy;
  }

  /** Cuts or restores every pod's database link (the database itself keeps running). */
  static void cutDatabase(boolean cut) {
    try {
      if (cut) {
        proxy().disable();
      } else {
        proxy().enable();
      }
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  /** Removes every toxic and restores the link: the next scenario starts clean. */
  static void healDatabase() {
    try {
      for (Toxic toxic : proxy().toxics().getAll()) {
        toxic.remove();
      }
      proxy().enable();
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  /** The broker, started on first use. */
  static synchronized KafkaContainer kafka() {
    if (kafka == null) {
      kafka = new KafkaContainer("apache/kafka-native:3.8.1");
      kafka.start();
    }
    return kafka;
  }

  /** Pauses (true) or resumes the broker - its ports stay, unlike a restart. */
  static void pauseKafka(boolean pause) {
    String id = kafka().getContainerId();
    if (pause) {
      DockerClientFactory.instance().client().pauseContainerCmd(id).exec();
    } else {
      DockerClientFactory.instance().client().unpauseContainerCmd(id).exec();
    }
  }

  static synchronized boolean migrated() {
    boolean was = migrated;
    migrated = true;
    return was;
  }

  /** Empties the outbox between scenarios (the schema stays). */
  static void truncate() {
    try (Connection c = connect();
        Statement st = c.createStatement()) {
      st.execute(
          "do $$ begin if to_regclass('outbox') is not null then"
              + " truncate table outbox restart identity; end if; end $$");
    } catch (SQLException ex) {
      throw new IllegalStateException(ex);
    }
  }
}
