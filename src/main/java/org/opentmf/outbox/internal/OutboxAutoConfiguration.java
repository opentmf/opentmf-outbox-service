package org.opentmf.outbox.internal;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.List;
import org.opentmf.outbox.OutboxClientProfileResolver;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxMaintenanceService;
import org.opentmf.outbox.OutboxProperties;
import org.opentmf.outbox.OutboxPublisher;
import org.opentmf.outbox.OutboxRelayedListener;
import org.opentmf.outbox.OutboxWriter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.thread.Threading;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

/**
 * Auto-configuration of the outbox starter: entity + repository registration, the writer,
 * the relay chain, the publisher SPI defaults (Kafka when a template exists; HTTP when
 * spring-web does) and the /ops trio (on by default; {@code opentmf.outbox.ops-endpoints=false}
 * disables). The consumer supplies: a DataSource/JPA, the Liquibase include of
 * {@code db/changelog/opentmf-outbox.sql}, security rows for /ops, and - optionally - its own
 * {@link OutboxPublisher} beans and an {@link OutboxClientProfileResolver}.
 */
@AutoConfiguration(
    afterName = {
      "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration",
      "org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration"
    })
// @AutoConfigurationPackage, NOT @EntityScan/@EnableJpaRepositories: an explicit scan
// declaration anywhere REPLACES Boot's default auto-configuration-package scanning, so the
// library's would have erased the CONSUMER's own entities and repositories (symptom: every
// consumer repository bean vanishes - "No qualifying bean of type ...Repository").
// This annotation instead ADDS org.opentmf.outbox to the default scan roots, so Hibernate and
// Spring Data pick up OutboxEvent + OutboxEventRepository BESIDE the consumer's own. Caveat a
// consumer that declares its OWN @EntityScan/@EnableJpaRepositories overrides the defaults and
// must then include this package explicitly.
@AutoConfigurationPackage(basePackageClasses = OutboxEvent.class)
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxAutoConfiguration {

  @Bean
  public OutboxWriter outboxWriter(
      OutboxEventRepository repository,
      ApplicationEventPublisher eventPublisher,
      ObjectMapper objectMapper,
      ObjectProvider<OutboxLaneStamper> laneStamper,
      OutboxClock clock) {
    // lazy: a consumer publisher that appends through the writer must not form a cycle
    return new OutboxWriter(
        repository, eventPublisher, objectMapper, laneStamper::getObject, clock);
  }

  /**
   * The outbox's time (1.9.0): the database's clock, as the JVM clock plus a measured offset - so
   * no pod's own clock decides a lease, a retry or a hold. A consumer's own bean replaces it.
   */
  @Bean
  @ConditionalOnMissingBean
  OutboxClock outboxClock(OutboxEventRepository repository) {
    return new OutboxClock(Clock.systemUTC(), repository::databaseNow);
  }

  /** Keeps the lease of every send in flight, up to its bound (1.9.0). */
  @Bean
  OutboxSendHeartbeat outboxSendHeartbeat(
      OutboxEventRepository repository,
      PlatformTransactionManager transactionManager,
      OutboxClock clock,
      OutboxProperties properties) {
    return new OutboxSendHeartbeat(
        repository, laneTransactions(transactionManager), clock, properties.getSendTimeout());
  }

  @Bean
  OutboxLaneStamper outboxLaneStamper(OutboxPublisherRouter router) {
    return new OutboxLaneStamper(router);
  }

  @Bean
  OutboxBackoff outboxBackoff(OutboxProperties properties) {
    return new OutboxBackoff(properties);
  }

  @Bean
  OutboxMetrics outboxMetrics(
      ObjectProvider<MeterRegistry> registry,
      OutboxEventRepository repository,
      OutboxProperties properties,
      OutboxClock clock) {
    // No registry bean (a consumer without actuator) = a simple local registry: the relay
    // keeps working, the gauges just have no exporter. Consumers with actuator get the real
    // one automatically.
    return new OutboxMetrics(
        registry.getIfAvailable(io.micrometer.core.instrument.simple.SimpleMeterRegistry::new),
        repository,
        properties.getMetricsRefresh(),
        clock);
  }

  @Bean
  OutboxPublisherRouter outboxPublisherRouter(List<OutboxPublisher> publishers) {
    return new OutboxPublisherRouter(publishers);
  }

  /**
   * The CONCURRENT lane: {@code max-in-flight} slots behind a hand-off queue of {@code batch-size}
   * rows; virtual threads when the runtime is 21+ AND the application enables
   * {@code spring.threads.virtual.enabled} (Boot's own test), platform threads otherwise.
   */
  @Bean
  OutboxConcurrentLane outboxConcurrentLane(OutboxProperties properties, Environment environment) {
    return new OutboxConcurrentLane(
        properties.getConcurrent().getMaxInFlight(),
        properties.getBatchSize(),
        Threading.VIRTUAL.isActive(environment),
        properties.getShutdownGrace());
  }

  /**
   * The ORDERED lane (1.8.0): one thread that sends a claimed ORDERED batch row after row, off the
   * relay thread - so an ORDERED batch never holds back the CONCURRENT lane's next claim.
   */
  @Bean
  OutboxOrderedLane outboxOrderedLane(OutboxProperties properties) {
    return new OutboxOrderedLane(properties.getShutdownGrace());
  }

  @Bean
  OutboxRelayWorker outboxRelayWorker(
      OutboxEventRepository repository,
      OutboxPublisherRouter router,
      OutboxBackoff backoff,
      OutboxMetrics metrics,
      OutboxProperties properties,
      ObjectProvider<OutboxRelayedListener> relayedListeners,
      PlatformTransactionManager transactionManager,
      OutboxConcurrentLane lane,
      OutboxOrderedLane orderedLane,
      OutboxClock clock,
      OutboxSendHeartbeat heartbeat) {
    // the post-relay seam: zero or more consumer beans, invoked in bean order inside the
    // booking transaction that stamps relayed_on (see OutboxRelayedListener)
    return new OutboxRelayWorker(
        repository,
        router,
        backoff,
        metrics,
        properties,
        relayedListeners.orderedStream().toList(),
        relayTransactions(transactionManager),
        laneTransactions(transactionManager),
        lane,
        orderedLane,
        clock,
        heartbeat);
  }

  /**
   * The relay's short claim transactions and the ORDERED lane's bookings, READ COMMITTED whatever
   * the application's default: the CONCURRENT claim's re-check must see, in a NEW snapshot, the
   * leases another pod committed before releasing a key's advisory lock - under REPEATABLE READ
   * the transaction's first snapshot would hide them; an ORDERED booking renews the NEXT row's
   * lease in the same transaction, and a serialization error there would roll back a delivered
   * row's booking. The pin costs a round trip per transaction (the driver reads the current level
   * from the server before setting it).
   */
  static TransactionTemplate relayTransactions(PlatformTransactionManager transactionManager) {
    TransactionTemplate template = new TransactionTemplate(transactionManager);
    template.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
    return template;
  }

  /**
   * The CONCURRENT lane's bookings and renewals (1.6.0), at the application's DEFAULT isolation -
   * READ COMMITTED on PostgreSQL unless the application configured another. Each is one guarded
   * statement whose snapshot starts with it (the failure booking: a locking read by id and lease,
   * then the update). Under another default, the one race - a relay re-taking the row after its
   * lease lapsed - raises a serialization error instead of matching nothing: the booking falls
   * through to the failure path, which finds the lease gone and books nothing - the same outcome.
   */
  static TransactionTemplate laneTransactions(PlatformTransactionManager transactionManager) {
    return new TransactionTemplate(transactionManager);
  }

  @Bean
  OutboxRelay outboxRelay(
      OutboxRelayWorker worker,
      OutboxProperties properties,
      OutboxConcurrentLane lane,
      OutboxOrderedLane orderedLane,
      OutboxSendHeartbeat heartbeat,
      OutboxClock clock) {
    return new OutboxRelay(worker, properties, lane, orderedLane, heartbeat, clock);
  }

  @Bean
  OutboxRelayTrigger outboxRelayTrigger(OutboxRelay relay) {
    return new OutboxRelayTrigger(relay);
  }

  @Bean
  public OutboxMaintenanceService outboxMaintenanceService(
      OutboxEventRepository repository,
      OutboxProperties properties,
      ApplicationEventPublisher eventPublisher,
      OutboxClock clock) {
    return new OutboxMaintenanceService(repository, properties, eventPublisher, clock);
  }

  /**
   * The publisher defaults live in NESTED, name-guarded configurations so this outer class
   * names no Kafka or web type in any bean-method signature. A method-level guard is not
   * enough: Spring evaluates it from ASM metadata and skips the bean, but still introspects the
   * enclosing class's methods reflectively to resolve the OTHER factory methods - and a type
   * missing from the consumer's classpath then throws {@code NoClassDefFoundError} and no
   * context starts (found on a Kafka-less consumer, 1.2.0). A class-level name-based guard is
   * evaluated BEFORE the nested class is loaded, so the guarded type is linked only when the
   * condition holds; the outer {@code afterName} ordering is inherited by the nested classes.
   *
   * <p>Deliberately NOT {@code @Configuration}: the nested classes are lite member classes
   * (bean methods only). A {@code @Configuration} nested class is a component-scan candidate,
   * and a consumer whose scan root covers {@code org.opentmf.outbox} (the library's own test
   * application is one) would register it AHEAD of the auto-configuration order - its
   * {@code @ConditionalOnBean(KafkaTemplate)} then evaluates before {@code KafkaAutoConfiguration}
   * has registered the template and the publisher silently vanishes ("No OutboxPublisher
   * supports destination"). A member class without the stereotype is invisible to the scan and
   * is processed only as part of this auto-configuration. {@code KafkaLessStartupTests} pins
   * all three facts.
   */
  @ConditionalOnClass(name = "org.springframework.kafka.core.KafkaTemplate")
  static class KafkaPublisherConfiguration {

    /** Kafka default publisher - LOWEST precedence so consumer publishers match first. */
    @Bean
    @Order(Ordered.LOWEST_PRECEDENCE)
    @ConditionalOnBean(KafkaTemplate.class)
    OutboxPublisher kafkaOutboxPublisher(
        KafkaTemplate<Object, Object> kafkaTemplate,
        OutboxProperties properties,
        ObjectMapper objectMapper,
        Environment environment) {
      return new KafkaOutboxPublisher(
          kafkaTemplate,
          properties,
          objectMapper,
          environment.getProperty("spring.application.name", "unknown"));
    }
  }

  @ConditionalOnClass(name = "org.springframework.web.client.RestClient")
  static class HttpPublisherConfiguration {

    /** HTTP publisher for http(s):// destinations - just above the Kafka fallback. */
    @Bean
    @Order(Ordered.LOWEST_PRECEDENCE - 1)
    OutboxPublisher httpOutboxPublisher(
        ObjectProvider<OutboxClientProfileResolver> profileResolver,
        ObjectMapper objectMapper,
        Environment environment,
        OutboxProperties properties) {
      return new HttpOutboxPublisher(
          OutboxHttpClients.plain(
              properties.getHttp().getConnectTimeout(), properties.httpReadTimeout()),
          profileResolver.getIfAvailable(),
          objectMapper,
          environment.getProperty("spring.application.name", "unknown"));
    }
  }

  /**
   * The /ops surface requires BOTH spring-web AND tmf630-toolkit-all on the classpath (the
   * controller's list endpoint is toolkit-rendered). The toolkit dependency is {@code optional},
   * and this guard is what makes that label honest — and the hazard it prevents is a QUIET one:
   * absent annotation classes are silently skipped by the JVM, so without the toolkit the
   * controller would still register and the list endpoint would serve WITHOUT its TMF630
   * rendering — a silent degradation no log line names. With the guard: no toolkit, no /ops
   * controller, a clean documented absence. Both conditions are name-based so this
   * auto-configuration class itself never links either. A toolkit-less consumer keeps the full
   * {@link org.opentmf.outbox.OutboxMaintenanceService} API and can wire its own endpoints.
   */
  @Bean
  @ConditionalOnClass(
      name = {
        "org.springframework.web.bind.annotation.RestController",
        "org.opentmf.query.tmf630.annotation.Tmf630Response"
      })
  @ConditionalOnProperty(
      name = "opentmf.outbox.ops-endpoints", havingValue = "true", matchIfMissing = true)
  public OutboxOpsController outboxOpsController(OutboxMaintenanceService maintenance) {
    return new OutboxOpsController(maintenance);
  }
}
