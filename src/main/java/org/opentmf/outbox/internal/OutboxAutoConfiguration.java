package org.opentmf.outbox.internal;

import io.micrometer.core.instrument.MeterRegistry;
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.client.RestClient;
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
      ObjectMapper objectMapper) {
    return new OutboxWriter(repository, eventPublisher, objectMapper);
  }

  @Bean
  OutboxBackoff outboxBackoff(OutboxProperties properties) {
    return new OutboxBackoff(properties);
  }

  @Bean
  OutboxMetrics outboxMetrics(
      ObjectProvider<MeterRegistry> registry,
      OutboxEventRepository repository,
      OutboxProperties properties) {
    // No registry bean (a consumer without actuator) = a simple local registry: the relay
    // keeps working, the gauges just have no exporter. Consumers with actuator get the real
    // one automatically.
    return new OutboxMetrics(
        registry.getIfAvailable(io.micrometer.core.instrument.simple.SimpleMeterRegistry::new),
        repository,
        properties);
  }

  @Bean
  OutboxPublisherRouter outboxPublisherRouter(List<OutboxPublisher> publishers) {
    return new OutboxPublisherRouter(publishers);
  }

  @Bean
  OutboxRelayWorker outboxRelayWorker(
      OutboxEventRepository repository,
      OutboxPublisherRouter router,
      OutboxBackoff backoff,
      OutboxMetrics metrics,
      OutboxProperties properties,
      ObjectProvider<OutboxRelayedListener> relayedListeners) {
    // the post-relay seam: zero or more consumer beans, invoked in bean order inside the
    // claim transaction (see OutboxRelayedListener)
    return new OutboxRelayWorker(
        repository, router, backoff, metrics, properties,
        relayedListeners.orderedStream().toList());
  }

  @Bean
  OutboxRelay outboxRelay(OutboxRelayWorker worker, OutboxProperties properties) {
    return new OutboxRelay(worker, properties);
  }

  @Bean
  OutboxRelayTrigger outboxRelayTrigger(OutboxRelay relay) {
    return new OutboxRelayTrigger(relay);
  }

  @Bean
  public OutboxMaintenanceService outboxMaintenanceService(
      OutboxEventRepository repository,
      OutboxProperties properties,
      ApplicationEventPublisher eventPublisher) {
    return new OutboxMaintenanceService(repository, properties, eventPublisher);
  }

  /** Kafka default publisher - LOWEST precedence so consumer publishers match first. */
  @Bean
  @Order(Ordered.LOWEST_PRECEDENCE)
  @ConditionalOnClass(KafkaTemplate.class)
  @ConditionalOnBean(KafkaTemplate.class)
  OutboxPublisher kafkaOutboxPublisher(
      KafkaTemplate<Object, Object> kafkaTemplate,
      OutboxProperties properties,
      ObjectMapper objectMapper,
      org.springframework.core.env.Environment environment) {
    return new KafkaOutboxPublisher(
        kafkaTemplate,
        properties,
        objectMapper,
        environment.getProperty("spring.application.name", "unknown"));
  }

  /** HTTP publisher for http(s):// destinations - just above the Kafka fallback. */
  @Bean
  @Order(Ordered.LOWEST_PRECEDENCE - 1)
  @ConditionalOnClass(RestClient.class)
  OutboxPublisher httpOutboxPublisher(
      ObjectProvider<OutboxClientProfileResolver> profileResolver,
      ObjectMapper objectMapper,
      org.springframework.core.env.Environment environment) {
    return new HttpOutboxPublisher(
        RestClient.create(),
        profileResolver.getIfAvailable(),
        objectMapper,
        environment.getProperty("spring.application.name", "unknown"));
  }

  @Bean
  @ConditionalOnClass(name = "org.springframework.web.bind.annotation.RestController")
  @ConditionalOnProperty(
      name = "opentmf.outbox.ops-endpoints", havingValue = "true", matchIfMissing = true)
  public OutboxOpsController outboxOpsController(OutboxMaintenanceService maintenance) {
    return new OutboxOpsController(maintenance);
  }
}
