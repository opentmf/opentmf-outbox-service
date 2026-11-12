package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import jakarta.persistence.EntityManagerFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.InterruptException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerPostProcessor;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * OUTBOX-TRANSACTIONAL-SEND-BOUND-1: a TRANSACTIONAL producer's send is several blocking calls -
 * {@code initTransactions} (a new producer: the first send, or the next one after a failure closed
 * it), the record's {@code send} (its metadata) and {@code commitTransaction} - each bounded by
 * {@code max.block.ms} on its own. Here each takes just under it, as a slow transaction coordinator
 * makes them: the producer itself never gives up, and the send succeeds. Each call is HELD until
 * the test releases it, once it has observed the call and let its time pass; every call's held time
 * is measured, so the stimulus is asserted, not assumed. The relay's bound for the
 * send must outlast that; up to 1.11.0 it counted {@code max.block.ms} once (delivery timeout +
 * linger + 1 s + max.block.ms), so with a lowered delivery timeout the heartbeat interrupted a send
 * that was going to succeed - and, the producer closed by the failure, every retry paid the
 * initialisation again and was interrupted again.
 */
@Testcontainers
@SpringBootTest(
    properties = {
      "spring.application.name=outbox-tx-send-bound-it",
      "spring.datasource.url=jdbc:tc:postgresql:18.1-alpine3.22:///txsendbound",
      "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
      "spring.liquibase.change-log=classpath:db/test-changelog.xml",
      "spring.jpa.hibernate.ddl-auto=validate",
      "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
      "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer",
      "spring.kafka.producer.transaction-id-prefix=it-tx-send-bound-",
      "spring.kafka.producer.properties[delivery.timeout.ms]=3000",
      "spring.kafka.producer.properties[request.timeout.ms]=2000",
      "spring.kafka.producer.properties[linger.ms]=0",
      "spring.kafka.producer.properties[max.block.ms]=" + KafkaTransactionalSendBoundIT.MAX_BLOCK_MS,
      "opentmf.outbox.send-timeout=1s",
      "opentmf.outbox.ordered.lease=2s",
      "opentmf.outbox.backoff-base=1s",
      "opentmf.outbox.sweep-interval=1s"
    })
@DirtiesContext
class KafkaTransactionalSendBoundIT {

  static final long MAX_BLOCK_MS = 5000;

  /** Each blocking call of a transactional send takes this long: under max.block.ms, never over. */
  static final Duration CALL = Duration.ofMillis(MAX_BLOCK_MS * 8 / 10);

  /** The 1.11.0 bound here: 3 s delivery timeout + 0 linger + 1 s + 5 s max.block.ms. */
  static final Duration BOUND_UP_TO_1_11_0 = Duration.ofSeconds(9);

  /** The longest a held call waits for its release: a test that never releases fails. */
  static final Duration HELD_AT_MOST = Duration.ofSeconds(30);

  /** The blocking calls as they arrive, for the test to release; and every one, for the record. */
  static final BlockingQueue<Held> CALLS = new LinkedBlockingQueue<>();

  static final List<Held> SEEN = new CopyOnWriteArrayList<>();

  static final String RELAYED_ON = "select relayed_on from outbox where id = ?";

  /** The producer calls that block up to max.block.ms each in a transactional send. */
  static final Set<String> BLOCKING = Set.of("initTransactions", "send", "commitTransaction");

  @Container static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.1");

  @DynamicPropertySource
  static void kafkaProps(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }

  @TestConfiguration(proxyBeanMethods = false)
  static class SlowCoordinator {

    /**
     * With a {@code transaction-id-prefix} Boot also registers a {@code KafkaTransactionManager}:
     * the JPA one is declared primary, as a consumer does - the append's database transaction.
     */
    @Bean
    @Primary
    PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
      return new JpaTransactionManager(entityManagerFactory);
    }

    @Bean
    @SuppressWarnings("unchecked") // Boot's producer factory is <Object, Object>
    DefaultKafkaProducerFactoryCustomizer slowBlockingCalls() {
      return factory ->
          ((DefaultKafkaProducerFactory<Object, Object>) factory).addPostProcessor(slowCalls());
    }
  }

  /** Every producer: each blocking call is held until the test releases it - interruptibly. */
  @SuppressWarnings("unchecked")
  static ProducerPostProcessor<Object, Object> slowCalls() {
    return producer ->
        (Producer<Object, Object>)
            Proxy.newProxyInstance(
                Producer.class.getClassLoader(),
                new Class<?>[] {Producer.class},
                (proxy, method, args) -> {
                  if (BLOCKING.contains(method.getName())) {
                    hold(method.getName());
                  }
                  try {
                    return method.invoke(producer, args);
                  } catch (InvocationTargetException ex) {
                    throw ex.getCause();
                  }
                });
  }

  /** One blocking producer call, held until the test releases it. */
  static final class Held {
    final String call;
    final long enteredAt = System.nanoTime();
    final CountDownLatch release = new CountDownLatch(1);

    /** How long the call was held, once it went on (released, interrupted or timed out). */
    volatile long heldNanos = -1;

    Held(String call) {
      this.call = call;
    }

    Duration held() {
      return Duration.ofNanos(heldNanos);
    }

    @Override
    public String toString() {
      return call + " held " + held();
    }
  }

  /** On the producer's thread: waits for the test's release, failing as Kafka on an interrupt. */
  static void hold(String call) {
    Held held = new Held(call);
    SEEN.add(held);
    CALLS.add(held);
    try {
      if (!held.release.await(HELD_AT_MOST.toMillis(), TimeUnit.MILLISECONDS)) {
        throw new IllegalStateException(call + " held " + HELD_AT_MOST + ", never released");
      }
    } catch (InterruptedException ex) {
      throw new InterruptException(ex); // re-sets the flag, as the client does
    } finally {
      held.heldNanos = System.nanoTime() - held.enteredAt;
    }
  }

  @Autowired private OutboxWriter writer;
  @Autowired private PlatformTransactionManager txManager;
  @Autowired private JdbcTemplate jdbc;

  private final ListAppender<ILoggingEvent> heartbeatLog = new ListAppender<>();
  private final Logger heartbeatLogger =
      (Logger) LoggerFactory.getLogger("org.opentmf.outbox.internal.OutboxSendHeartbeat");

  @BeforeEach
  void listen() {
    CALLS.clear();
    SEEN.clear();
    heartbeatLog.start();
    heartbeatLogger.addAppender(heartbeatLog);
  }

  @AfterEach
  void stopListening() {
    heartbeatLogger.detachAppender(heartbeatLog);
  }

  /**
   * One row through a transactional producer whose three blocking calls each take 4 s of their
   * 5 s: about 12 s, past the 1.11.0 bound of 9 s, within the producer's own limits. It is relayed
   * on its first attempt, never presumed hung.
   */
  @Test
  void aTransactionalSendWhoseCallsEachBlockUnderMaxBlock_isNotInterrupted()
      throws InterruptedException {
    String topic = "tx-send-bound-" + UUID.randomUUID();
    String aggregate = "agg-" + UUID.randomUUID();
    long id =
        new TransactionTemplate(txManager)
            .execute(s -> writer.append("thing", aggregate, "thing.created.v1", topic, Map.of()))
            .getId();

    releaseEachCallAfter(CALL, id, Duration.ofSeconds(60));

    List<Held> first = List.copyOf(SEEN).subList(0, 3); // a retry may still be adding calls
    assertThat(first)
        .as("the first send's blocking calls, in order")
        .extracting(held -> held.call)
        .containsExactly("initTransactions", "send", "commitTransaction");
    assertThat(first)
        .as("each held for its time, within max.block.ms")
        .allSatisfy(
            held ->
                assertThat(held.held())
                    .isGreaterThanOrEqualTo(CALL)
                    .isLessThan(Duration.ofMillis(MAX_BLOCK_MS)));
    assertThat(
            first.stream().map(Held::held).reduce(Duration.ZERO, Duration::plus))
        .as("together past the 1.11.0 bound")
        .isGreaterThan(BOUND_UP_TO_1_11_0);
    try (KafkaProbe probe = KafkaProbe.committed(kafka.getBootstrapServers(), topic)) {
      assertThat(probe.drain(1, Duration.ofSeconds(10))).as("committed on the topic").hasSize(1);
    }
    assertThat(heartbeatLog.list)
        .as("no send was presumed hung")
        .noneMatch(event -> event.getFormattedMessage().contains("presumed hung"));
    Map<String, Object> row =
        jdbc.queryForMap("select attempts, relayed_on from outbox where id = ?", id);
    assertThat(row.get("relayed_on")).isNotNull();
    assertThat(row).containsEntry("attempts", 0); // on its first attempt
  }

  /**
   * Releases each blocking call once {@code call} has passed since it began (sooner if it went on
   * without the release - interrupted), until the row is relayed or {@code atMost} has passed.
   */
  private void releaseEachCallAfter(Duration call, long id, Duration atMost)
      throws InterruptedException {
    long deadline = System.nanoTime() + atMost.toNanos();
    while (System.nanoTime() < deadline
        && jdbc.queryForObject(RELAYED_ON, Object.class, id) == null) {
      Held next = CALLS.poll(200, TimeUnit.MILLISECONDS);
      if (next == null) {
        continue;
      }
      await()
          .pollInterval(Duration.ofMillis(10))
          .atMost(call.plusSeconds(5))
          .until(
              () ->
                  next.heldNanos >= 0 || System.nanoTime() - next.enteredAt >= call.toNanos());
      next.release.countDown();
    }
  }
}
