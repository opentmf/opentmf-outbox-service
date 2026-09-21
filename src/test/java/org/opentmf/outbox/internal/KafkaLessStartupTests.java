package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxWriter;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.core.annotation.MergedAnnotations.SearchStrategy;
import org.springframework.stereotype.Component;

/**
 * A consumer WITHOUT {@code spring-kafka} (or without {@code spring-web}) must start.
 *
 * <p>The hazard is reflective, not conditional: Spring evaluates a method-level {@code
 * @ConditionalOnClass} from ASM metadata and does skip the bean, but it still introspects the
 * auto-configuration class's methods reflectively ({@code getDeclaredMethods()}) to resolve the
 * OTHER factory methods — and a Kafka (or web) type in ANY bean-method signature then throws
 * {@code NoClassDefFoundError} and no context starts (the dnms-catalog 1.2.0 symptom). The
 * publisher defaults therefore live in NESTED, name-guarded configurations, and the outer class
 * names neither type.
 *
 * <p>Boot's {@code FilteredClassLoader} cannot reproduce this: it delegates every definition to
 * the app loader, which CAN see Kafka, so only the {@code @ConditionalOnClass} lookup is fooled.
 * {@link ConsumerClassLoader} instead DEFINES the library classes itself and refuses the hidden
 * package, exactly as an absent jar would; the configurations are registered by NAME so Spring
 * resolves them through it. The repository is a mock: the subject is what the auto-configuration
 * links, not JPA.
 */
class KafkaLessStartupTests {

  private static final String LIBRARY_PACKAGE = "org.opentmf.outbox.";
  private static final String KAFKA_PACKAGE = "org.springframework.kafka";
  private static final String WEB_PACKAGE = "org.springframework.web";

  @Configuration(proxyBeanMethods = false)
  static class ConsumerWithoutJpa {
    @Bean
    OutboxEventRepository outboxEventRepository() {
      return mock(OutboxEventRepository.class);
    }
  }

  /** Child-first for the library (main + test classes), parent-first otherwise, hidden: none. */
  static final class ConsumerClassLoader extends URLClassLoader {

    private final String hiddenPackage;

    ConsumerClassLoader(String hiddenPackage) {
      super(
          new URL[] {
            OutboxAutoConfiguration.class.getProtectionDomain().getCodeSource().getLocation(),
            KafkaLessStartupTests.class.getProtectionDomain().getCodeSource().getLocation()
          },
          KafkaLessStartupTests.class.getClassLoader());
      this.hiddenPackage = hiddenPackage;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (name.startsWith(hiddenPackage)) {
        throw new ClassNotFoundException(name + " is not on this consumer's classpath");
      }
      if (!name.startsWith(LIBRARY_PACKAGE)) {
        return super.loadClass(name, resolve);
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          loaded = findClass(name);
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }
  }

  private static ApplicationContextRunner consumerWithout(String hiddenPackage) {
    ConsumerClassLoader loader = new ConsumerClassLoader(hiddenPackage);
    return new ApplicationContextRunner()
        .withClassLoader(loader)
        .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class))
        .withInitializer(
            context -> {
              GenericApplicationContext registry = (GenericApplicationContext) context;
              registry.registerBeanDefinition(
                  "consumerWithoutJpa", byName(ConsumerWithoutJpa.class.getName()));
              registry.registerBeanDefinition(
                  "outboxAutoConfiguration", byName(OutboxAutoConfiguration.class.getName()));
            });
  }

  /** A name-only definition: the class is resolved through the context's loader, never ours. */
  private static BeanDefinition byName(String className) {
    return BeanDefinitionBuilder.genericBeanDefinition(className).getBeanDefinition();
  }

  /** By NAME: the bean's class comes from the consumer loader, never identical to ours. */
  private static void assertBeanIs(ApplicationContext context, String name, Class<?> type) {
    assertThat(context.getBean(name).getClass().getName()).isEqualTo(type.getName());
  }

  @Test
  void aKafkaLessConsumerStarts_withTheHttpPublisherAndWithoutTheKafkaOne() {
    consumerWithout(KAFKA_PACKAGE)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertBeanIs(context, "outboxWriter", OutboxWriter.class);
              assertBeanIs(context, "outboxOpsController", OutboxOpsController.class);
              assertBeanIs(context, "httpOutboxPublisher", HttpOutboxPublisher.class);
              assertThat(context).doesNotHaveBean("kafkaOutboxPublisher");
            });
  }

  @Test
  void aWebLessConsumerStarts_withoutTheHttpPublisher() {
    consumerWithout(WEB_PACKAGE)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertBeanIs(context, "outboxWriter", OutboxWriter.class);
              assertBeanIs(context, "outboxRelayTrigger", OutboxRelayTrigger.class);
              assertThat(context).doesNotHaveBean("httpOutboxPublisher");
              assertThat(context).doesNotHaveBean("kafkaOutboxPublisher");
            });
  }

  /**
   * A {@code @Configuration} nested class is a component-scan candidate: a consumer scanning
   * {@code org.opentmf.outbox} (the ITs' test application does) would register it ahead of the
   * auto-configuration order, and {@code @ConditionalOnBean(KafkaTemplate)} would evaluate
   * before {@code KafkaAutoConfiguration} exists - the publisher would silently vanish. Lite
   * member classes (bean methods, no stereotype) are processed only through the outer class.
   */
  @Test
  void theNestedPublisherConfigurations_carryNoStereotype_soAScanCannotRegisterThemEarly() {
    for (Class<?> nested : OutboxAutoConfiguration.class.getDeclaredClasses()) {
      boolean stereotyped =
          MergedAnnotations.from(nested, SearchStrategy.TYPE_HIERARCHY).isPresent(Component.class);
      assertThat(stereotyped)
          .as(nested.getSimpleName() + " must not be a component-scan candidate")
          .isFalse();
    }
  }

  @Test
  void theOuterAutoConfigurationNamesNoKafkaOrWebTypeInAnySignature() {
    for (Method m : OutboxAutoConfiguration.class.getDeclaredMethods()) {
      for (Class<?> t : m.getParameterTypes()) {
        assertThat(t.getName()).as(m.getName()).doesNotStartWith(KAFKA_PACKAGE);
        assertThat(t.getName()).as(m.getName()).doesNotStartWith(WEB_PACKAGE);
      }
      assertThat(m.getReturnType().getName()).as(m.getName()).doesNotStartWith(KAFKA_PACKAGE);
      assertThat(m.getReturnType().getName()).as(m.getName()).doesNotStartWith(WEB_PACKAGE);
    }
  }
}
