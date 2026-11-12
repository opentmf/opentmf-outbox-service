package org.opentmf.outboxinvariants;

import jakarta.persistence.EntityManagerFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.InterruptException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * A pod whose transactional producers hang in {@code initTransactions} (E4, on {@code
 * it.kafka.held-init=true} only): every call waits - interruptibly, failing as the Kafka client
 * does - until the scenario {@link #release releases} them, so every send outruns its bound and the
 * send heartbeat interrupts it. The broker itself stays healthy throughout.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "it.kafka.held-init", havingValue = "true")
class KafkaHeldInit {

  /** The longest a held call waits: a scenario that never releases fails, it never hangs. */
  static final Duration HELD_AT_MOST = Duration.ofMinutes(2);

  private static volatile boolean released;

  /** The next scenario's producers hang again. */
  static void hold() {
    released = false;
  }

  /** From now on {@code initTransactions} runs at once. */
  static void release() {
    released = true;
  }

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
  DefaultKafkaProducerFactoryCustomizer heldInit() {
    return factory ->
        ((DefaultKafkaProducerFactory<Object, Object>) factory)
            .addPostProcessor(
                producer ->
                    (Producer<Object, Object>)
                        Proxy.newProxyInstance(
                            Producer.class.getClassLoader(),
                            new Class<?>[] {Producer.class},
                            (proxy, method, args) -> {
                              if ("initTransactions".equals(method.getName())) {
                                awaitRelease();
                              }
                              try {
                                return method.invoke(producer, args);
                              } catch (InvocationTargetException ex) {
                                throw ex.getCause();
                              }
                            }));
  }

  private static void awaitRelease() {
    long end = System.nanoTime() + HELD_AT_MOST.toNanos();
    while (!released) {
      if (System.nanoTime() - end > 0) {
        throw new IllegalStateException(
            "initTransactions held " + HELD_AT_MOST + " and never released - the scenario failed");
      }
      LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
      if (Thread.interrupted()) {
        throw new InterruptException(new InterruptedException()); // re-sets the flag, as Kafka
      }
    }
  }
}
