package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.LongStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * OUTBOX-IDEMPOTENCY-KEY-REPEATS-AFTER-RESTORE-1: the {@code x-idempotency-key} names the row, not
 * only its id. An id is reused once the table is recreated or the database is restored to a point
 * in time - every key up to 1.11.0 a consumer already holds then marks a NEW event as a duplicate,
 * which the consumer drops without a word. The receiver here is the consumer's dedup memory, kept
 * OUTSIDE the restored database, as a consumer's own is.
 *
 * <ul>
 *   <li>(a) a recreated table: the new rows' keys are new;
 *   <li>(b) a restore to a point in time T with rows sent after it: the rows booked after the
 *       restore reuse the ids sent before it, never their keys;
 *   <li>(c) a row pending at T, sent before the restore and sent again after it, carries the SAME
 *       key both times: still recognised as the same event (green up to 1.11.0 as well - the guard
 *       that the fix keeps it).
 * </ul>
 *
 * <p>The restore is the database's effect, reproduced by hand: the table's rows as they were at T
 * (copied aside at T) and its identity back where it stood. The real point-in-time recovery (WAL
 * archive, base backup, {@code recovery_target_name}, a new timeline) measured the same ids
 * repeating, 11 to 110 for 100 bookings after a restore to id 10.
 */
@SpringBootTest(
    properties = {
      "spring.application.name=outbox-key-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///keyrestore",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "opentmf.outbox.sweep-interval=1s"
    })
@DirtiesContext
class OutboxIdempotencyKeyRestoreIT {

  /** Every key the consumer has received, in arrival order: its dedup memory. */
  private static final List<String> RECEIVED = new CopyOnWriteArrayList<>();

  private static HttpServer receiver;
  private static String destination;

  @Autowired private OutboxWriter writer;
  @Autowired private OutboxMaintenanceService maintenance;
  @Autowired private JdbcTemplate jdbc;
  @Autowired private PlatformTransactionManager txManager;

  @BeforeAll
  static void startReceiver() throws IOException {
    receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    receiver.createContext(
        "/consumer",
        exchange -> {
          RECEIVED.add(exchange.getRequestHeaders().getFirst(OutboxHeaders.IDEMPOTENCY_KEY));
          exchange.getRequestBody().readAllBytes();
          exchange.sendResponseHeaders(200, -1);
          exchange.close();
        });
    receiver.start();
    destination = "http://127.0.0.1:" + receiver.getAddress().getPort() + "/consumer";
  }

  @AfterAll
  static void stopReceiver() {
    receiver.stop(0);
  }

  /** Each case starts on an empty table at id 1 and a consumer that remembers nothing. */
  @BeforeEach
  void anEmptyTableAndAFreshConsumer() {
    jdbc.execute("drop table if exists outbox_at_t");
    jdbc.execute("truncate outbox restart identity");
    RECEIVED.clear();
  }

  @Test
  void a_aRecreatedTable_givesTheNewRowsNewKeys() {
    List<Long> before = bookAndDeliver(5);
    assertThat(before).containsExactly(1L, 2L, 3L, 4L, 5L);
    Set<String> remembered = new HashSet<>(RECEIVED);

    jdbc.execute("truncate outbox restart identity"); // the table dropped and created again

    List<Long> after = bookAndDeliver(5);
    assertThat(after).as("the ids are reused").containsExactlyElementsOf(before);
    assertThat(RECEIVED.subList(remembered.size(), RECEIVED.size()))
        .as("no new row's key is one the consumer already holds")
        .hasSize(5)
        .doesNotContainAnyElementsOf(remembered);
  }

  @Test
  void b_aRestoreToAPointInTime_givesTheRowsBookedAfterItNewKeys() {
    bookAndDeliver(5); // ids 1-5 sent
    List<Long> pendingAtT = bookHeld(5); // ids 6-10 pending at T
    pointInTimeT();
    release(pendingAtT.subList(0, 3)); // after T: 6-8 sent ...
    List<Long> sentAfterT = bookAndDeliver(20); // ... and 20 new rows, ids 11-30
    assertThat(sentAfterT).first().isEqualTo(11L);
    Set<String> remembered = new HashSet<>(RECEIVED);

    restoreToT();

    List<Long> bookedAfterRestore = bookAndDeliver(20);
    assertThat(bookedAfterRestore)
        .as("the restore brought the identity back: the ids repeat")
        .containsExactlyElementsOf(sentAfterT);
    assertThat(RECEIVED.subList(remembered.size(), RECEIVED.size()))
        .as("no row booked after the restore carries a key the consumer already holds")
        .hasSize(20)
        .doesNotContainAnyElementsOf(remembered);
  }

  @Test
  void c_aRowPendingAtT_sentAgainAfterTheRestore_keepsItsKey() {
    bookAndDeliver(5);
    List<Long> pendingAtT = bookHeld(5);
    pointInTimeT();
    List<Long> sentBeforeTheFailure = pendingAtT.subList(0, 3);
    release(sentBeforeTheFailure);
    List<String> firstSend = new ArrayList<>(RECEIVED.subList(5, 8));

    restoreToT(); // 6-8 are pending again: their sends after T were lost with the database
    int mark = RECEIVED.size();
    release(sentBeforeTheFailure);

    assertThat(RECEIVED.subList(mark, RECEIVED.size()))
        .as("the same event, sent again: the consumer recognises every one")
        .containsExactlyInAnyOrderElementsOf(firstSend);
  }

  // ---------------------------------------------------------------- steps

  private List<Long> bookAndDeliver(int rows) {
    List<Long> ids = book(rows, null);
    awaitRelayed(ids);
    return ids;
  }

  /** Rows held for an hour: pending, not claimable, until {@link #release}. */
  private List<Long> bookHeld(int rows) {
    return book(rows, OffsetDateTime.now().plusHours(1));
  }

  private List<Long> book(int rows, OffsetDateTime releaseAt) {
    TransactionTemplate tx = new TransactionTemplate(txManager);
    return LongStream.range(0, rows)
        .mapToObj(
            n ->
                tx.execute(
                    s ->
                        writer
                            .append(
                                OutboxAppend.of(
                                        "key", "agg-" + n, "key.v1", destination, Map.of("n", n))
                                    .withReleaseAt(releaseAt))
                            .getId()))
        .toList();
  }

  /** The hold passes: the rows are delivered. */
  private void release(List<Long> ids) {
    ids.forEach(id -> jdbc.update("update outbox set release_at = now() where id = ?", id));
    awaitRelayed(ids);
  }

  private void awaitRelayed(List<Long> ids) {
    await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> ids.stream().allMatch(id -> maintenance.inspect(id).relayedOn() != null));
  }

  /** T: the rows as they are now, copied aside - what a backup taken now restores. */
  private void pointInTimeT() {
    jdbc.execute("create table outbox_at_t as select * from outbox");
  }

  /** The restore to T: the rows as they were at T, the identity where it stood at T. */
  private void restoreToT() {
    Long last = jdbc.queryForObject("select max(id) from outbox_at_t", Long.class);
    jdbc.execute("truncate outbox");
    jdbc.execute("insert into outbox overriding system value select * from outbox_at_t");
    jdbc.execute("alter table outbox alter column id restart with " + (last + 1));
  }
}
