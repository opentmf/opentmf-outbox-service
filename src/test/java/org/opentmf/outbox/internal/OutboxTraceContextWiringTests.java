package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxEvent;
import org.opentmf.outbox.OutboxWriter;
import org.opentmf.outbox.fixture.TestTracing;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * The auto-configured writer and the application's tracing: with a {@code Tracer} and a {@code
 * Propagator} - what Boot's tracing auto-configuration registers - an append inside a span stores
 * that span's {@code traceparent}; {@code opentmf.outbox.stamp-trace-context=false} stores none;
 * an application's own {@link OutboxTraceContext} replaces the library's.
 */
class OutboxTraceContextWiringTests {

  private static final TestTracing TRACING = new TestTracing();

  @Configuration(proxyBeanMethods = false)
  static class Consumer {
    @Bean
    OutboxEventRepository outboxEventRepository() {
      OutboxEventRepository repository = mock(OutboxEventRepository.class);
      when(repository.save(any()))
          .thenAnswer(
              invocation -> {
                OutboxEvent event = invocation.getArgument(0);
                event.setId(1L);
                return event;
              });
      return repository;
    }

    @Bean
    PlatformTransactionManager transactionManager() {
      return mock(PlatformTransactionManager.class);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class Tracing {
    @Bean
    Tracer tracer() {
      return TRACING.tracer();
    }

    @Bean
    Propagator propagator() {
      return TRACING.propagator();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class OwnTraceContext {
    @Bean
    OutboxTraceContext ownTraceContext() {
      return () -> Map.of("traceparent", "00-own-01");
    }
  }

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(JacksonAutoConfiguration.class, OutboxAutoConfiguration.class))
          .withUserConfiguration(Consumer.class);

  /** An append inside a new span; the span's id beside the stored headers. */
  private static String[] appendInASpan(AssertableApplicationContext context) {
    Tracer tracer = TRACING.tracer();
    Span span = tracer.nextSpan().name("request").start();
    try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
      OutboxEvent row =
          context
              .getBean(OutboxWriter.class)
              .append("thing", "a-1", "thing.created.v1", "topic", Map.of());
      return new String[] {span.context().spanId(), row.getHeaders()};
    } finally {
      span.end();
    }
  }

  @Test
  void withTheApplicationsTracing_anAppendInsideASpanStoresThatSpan() {
    runner
        .withUserConfiguration(Tracing.class)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(context.getBean("outboxTraceContext"))
                  .isInstanceOf(OutboxTracerContext.class);
              String[] appended = appendInASpan(context);
              assertThat(appended[1]).contains("\"traceparent\"", appended[0]);
            });
  }

  @Test
  void switchedOff_anAppendInsideASpanStoresNothing() {
    runner
        .withUserConfiguration(Tracing.class)
        .withPropertyValues("opentmf.outbox.stamp-trace-context=false")
        .run(context -> assertThat(appendInASpan(context)[1]).isNull());
  }

  @Test
  void withoutTheApplicationsTracer_anAppendStoresNothing() {
    runner.run(
        context -> {
          assertThat(context).hasNotFailed();
          assertThat(appendInASpan(context)[1]).isNull();
        });
  }

  @Test
  void anApplicationsOwnTraceContext_replacesTheLibrarys() {
    runner
        .withUserConfiguration(Tracing.class, OwnTraceContext.class)
        .run(
            context -> {
              assertThat(context).doesNotHaveBean("outboxTraceContext");
              assertThat(appendInASpan(context)[1]).contains("00-own-01");
            });
  }
}
