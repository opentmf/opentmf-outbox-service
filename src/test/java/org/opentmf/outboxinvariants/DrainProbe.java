package org.opentmf.outboxinvariants;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.opentmf.outboxinvariants.Receiver.Delivery;

/**
 * The drain around the claim, written down (OUTBOX-D3-FAIRNESS-INVESTIGATION-1), beside the pods'
 * {@link ClaimProbe} files: each prune batch ({@code prune.csv}), once a second the table's dead
 * tuples, autovacuum and the host's load ({@code samples.csv}), and at the end every delivery
 * ({@code deliveries.csv}).
 */
final class DrainProbe implements AutoCloseable {

  private final Path dir;
  private final PrintWriter prune;
  private final PrintWriter samples;
  private final ScheduledExecutorService sampler =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread thread = new Thread(r, "drain-probe");
            thread.setDaemon(true);
            return thread;
          });

  private DrainProbe(Path dir) throws IOException {
    this.dir = dir;
    Files.createDirectories(dir);
    prune = new PrintWriter(Files.newBufferedWriter(dir.resolve("prune.csv")), true);
    prune.println("start_us,took_us,pruned");
    samples = new PrintWriter(Files.newBufferedWriter(dir.resolve("samples.csv")), true);
    samples.println("at_us,load1,n_live_tup,n_dead_tup,autovacuum_count,autovacuum_workers");
    sampler.scheduleAtFixedRate(this::sample, 0, 1, TimeUnit.SECONDS);
  }

  static DrainProbe open(Path dir) {
    try {
      return new DrainProbe(dir);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  /** The instant every D3 row became due: the drain's zero. */
  void eligible(Instant eligible) {
    samples.println("# eligible_us " + micros(eligible));
  }

  void pruned(long startMicros, long tookMicros, long rows) {
    prune.printf("%d,%d,%d%n", startMicros, tookMicros, rows);
  }

  private void sample() {
    try (Connection c = Infra.connect();
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                """
                select t.n_live_tup, t.n_dead_tup, t.autovacuum_count,
                  (select count(*) from pg_stat_activity a
                   where a.backend_type = 'autovacuum worker')
                from pg_stat_user_tables t where t.relname = 'outbox'""")) {
      String load = Files.readString(Path.of("/proc/loadavg")).split(" ")[0];
      if (rs.next()) {
        samples.printf(
            "%d,%s,%d,%d,%d,%d%n",
            Instant.now().toEpochMilli() * 1000,
            load,
            rs.getLong(1),
            rs.getLong(2),
            rs.getLong(3),
            rs.getLong(4));
      }
    } catch (SQLException | IOException ex) {
      samples.println("# " + ex);
    }
  }

  @Override
  public void close() throws IOException {
    sampler.shutdownNow();
    prune.close();
    samples.close();
    try (PrintWriter out =
        new PrintWriter(Files.newBufferedWriter(dir.resolve("deliveries.csv")))) {
      out.println("row_id,key,pod,attempt,start_us,end_us,delivered");
      for (Delivery d : Receiver.deliveries()) {
        out.printf(
            "%d,%s,%s,%d,%d,%d,%s%n",
            d.rowId(),
            d.destination(),
            d.pod(),
            d.attempt(),
            micros(d.start()),
            micros(d.end()),
            d.delivered());
      }
    }
  }

  private static long micros(Instant instant) {
    return instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1000;
  }
}
