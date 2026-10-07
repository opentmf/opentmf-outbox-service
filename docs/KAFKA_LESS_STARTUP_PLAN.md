# opentmf-outbox-service 1.2.1 — a Kafka-less consumer must start

**Status:** planned 2026-09-11; performed 2026-09-21 (see §4 "As performed").
**Found by:** Yusuf, on dnms-catalog 1.2.0 (the first consumer without Kafka on the classpath).
**Severity:** blocker for any Kafka-less consumer; no impact on the six existing consumers
(dnms-681, dnms-flow, the email/inbox/sms adapters, the template), all of which carry Kafka.

## 1. The defect

`OutboxAutoConfiguration` declares the Kafka default publisher as a `@Bean` method **of the
auto-configuration class itself**, with the guard at METHOD level:

```java
@Bean
@Order(Ordered.LOWEST_PRECEDENCE)
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnBean(KafkaTemplate.class)
OutboxPublisher kafkaOutboxPublisher(KafkaTemplate<Object, Object> kafkaTemplate, ...)
```

`spring-kafka` is `<optional>true</optional>` (pom.xml:126-127). A method-level
`@ConditionalOnClass` is evaluated from ASM metadata and does skip the bean — but Spring must
still **introspect the configuration class's methods reflectively** to register the other
beans, and the method SIGNATURE names `KafkaTemplate`. In a consumer without spring-kafka that
introspection throws `NoClassDefFoundError: org/springframework/kafka/core/KafkaTemplate`, and
no context starts. The `/ops` guard on `outboxOpsController` avoids exactly this by being
name-based and by keeping the guarded types out of the signature (see
`OutboxOpsOptionalityTests`); the Kafka bean never got the same treatment because every
consumer until dnms-catalog 1.2.0 had Kafka.

The HTTP publisher is safe by accident: `RestClient` appears only in the method BODY
(`RestClient.create()`), which links lazily, and its `@ConditionalOnClass(RestClient.class)` is a
class literal on a method — see §3 for why it is tightened in the same release.

## 2. The fix

Move the Kafka publisher into a **nested, class-guarded configuration** so the outer
auto-configuration class never references a Kafka type in any signature:

```java
// in OutboxAutoConfiguration — the outer class loses the KafkaTemplate import entirely
@Configuration(proxyBeanMethods = false)
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
        kafkaTemplate, properties, objectMapper,
        environment.getProperty("spring.application.name", "unknown"));
  }
}
```

Why this shape works: Spring evaluates a class-level `@ConditionalOnClass` on the nested
class from ASM metadata **before** loading it, so `KafkaTemplate` is linked only when the
condition holds. The `name = "..."` form keeps the annotation itself free of the class
literal (the same rule `OutboxOpsOptionalityTests` enforces for `/ops`). `@ConditionalOnBean`
stays on the method: it needs the bean registry, which exists by then, and it must keep
running AFTER `KafkaAutoConfiguration` — the outer class's `afterName` ordering is inherited by
the nested configuration.

`KafkaOutboxPublisher` itself is unchanged; it is only ever loaded through the nested class.

## 3. Same release, same reason: tighten the HTTP publisher's guard

`httpOutboxPublisher` carries `@ConditionalOnClass(RestClient.class)` — a class literal in a
method annotation. It has not bitten because `RestClient` is on every consumer's classpath and
the parameter types are Spring-core, but it is the same hazard one refactor away. Change it to
`@ConditionalOnClass(name = "org.springframework.web.client.RestClient")` and move the bean into
a nested `HttpPublisherConfiguration` guarded the same way, so a web-less consumer (a pure
Kafka relay) also starts. Behaviour is identical for every current consumer.

## 4. The red-first test (write it first, watch it fail on 1.2.0's shape)

Boot's `FilteredClassLoader` reproduces Yusuf's stack without touching the pom:

```java
class KafkaLessStartupTests {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(OutboxAutoConfiguration.class))
          .withUserConfiguration(JpaTestSupport.class)  // the existing DataSource/JPA fixture
          .withClassLoader(new FilteredClassLoader(KafkaTemplate.class));

  @Test
  void aKafkaLessConsumerStarts_withTheHttpPublisherAndWithoutTheKafkaOne() {
    runner.run(context -> {
      assertThat(context).hasNotFailed();
      assertThat(context).hasSingleBean(OutboxWriter.class);
      assertThat(context).hasBean("httpOutboxPublisher");
      assertThat(context).doesNotHaveBean("kafkaOutboxPublisher");
    });
  }

  @Test
  void theOuterAutoConfigurationNamesNoKafkaTypeInAnySignature() {
    for (Method m : OutboxAutoConfiguration.class.getDeclaredMethods()) {
      for (Class<?> t : m.getParameterTypes()) {
        assertThat(t.getName()).doesNotStartWith("org.springframework.kafka");
      }
      assertThat(m.getReturnType().getName()).doesNotStartWith("org.springframework.kafka");
    }
  }
}
```

On 1.2.0's shape the first test fails with the `NoClassDefFoundError` (the exact symptom); the
second fails on `kafkaOutboxPublisher`'s parameter. Both go green with §2. Add the mirror
case with `FilteredClassLoader(RestClient.class)` for §3. `ProfileAdapterKafkaOrderIT` and
`KafkaOutboxPublisherTests` stay as they are — they prove the Kafka path when Kafka IS present.

**As performed (2026-09-21) — `FilteredClassLoader` does NOT reproduce the defect.** On
1.2.0's shape the two startup tests above PASSED; only the signature test went red. Boot's
`FilteredClassLoader` has no URLs of its own and delegates every class definition to the app
loader, which CAN see `KafkaTemplate`; it only fools the `@ConditionalOnClass` lookup
(`Class.forName` through the context loader). The failing step is the reflective
`getDeclaredMethods()` on `OutboxAutoConfiguration`, and that resolves parameter types through
the class's DEFINING loader — the app loader — so no `NoClassDefFoundError` occurs.
`KafkaLessStartupTests` therefore uses a small child-first `ConsumerClassLoader` (over the
`target/classes` and `target/test-classes` code sources of the library, located via
`getProtectionDomain().getCodeSource()`, not classpath order) that DEFINES every
`org.opentmf.outbox.*` class itself and throws `ClassNotFoundException` for the hidden package
(`org.springframework.kafka`, or `org.springframework.web` for the §3 mirror); the two
configurations are registered by NAME so Spring resolves them through it. With that loader the
Kafka-less test failed on 1.2.0's shape with the real stack —
`IllegalStateException: Failed to introspect Class [org.opentmf.outbox.internal.OutboxAutoConfiguration]`
`Caused by: NoClassDefFoundError: org/springframework/kafka/core/KafkaTemplate` — and went green
with §2. There is no `JpaTestSupport` fixture in this repo; the repository is a Mockito mock,
since the subject is what the auto-configuration links, not JPA. The web-less mirror passed on
1.2.0's shape too (the "safe by accident" of §1) and still passes after §3.

**As performed — the nested classes carry NO `@Configuration`.** With the §2 shape verbatim
(`@Configuration(proxyBeanMethods = false)` on the nested classes) the Kafka ITs failed:
"No OutboxPublisher supports destination". The ITs' `OutboxTestApplication` lives in
`org.opentmf.outbox`, and its component scan registers a nested `@Configuration` class directly
(the outer class is scan-excluded as a listed auto-configuration; nested stereotypes are not),
ahead of the auto-configuration order — `@ConditionalOnBean(KafkaTemplate)` then evaluates before
`KafkaAutoConfiguration` has loaded its definitions, the bean is skipped, and the later
member-class copy is ignored in favour of the scanned one. The same would hit any consumer whose
scan root covers the library package. The nested classes are therefore lite member classes
(`@ConditionalOnClass(name = …)` + `@Bean` methods, no stereotype): Spring still processes them
as members of the outer auto-configuration in its ordering, but a scan cannot see them.
`KafkaLessStartupTests` pins that no nested class is `@Component`-meta-annotated.

## 5. Release checklist

1. Branch `fix/kafka-less-startup` off `develop` (currently 1.2.1-SNAPSHOT after 7b2fdd2).
2. §4 tests red → §2 + §3 → green; `mvn -Pmutation verify` (the pom's mutation profile) green.
3. `CHANGELOG.md`: new section `## 1.2.1 - <date>` (bare numeric, SNAPSHOT stripped):
   - **Fixed** — a consumer without `spring-kafka` could not start: `OutboxAutoConfiguration`
     named `KafkaTemplate` in a bean-method signature; the Kafka publisher now lives in a nested,
     name-guarded configuration. Found on dnms-catalog 1.2.0 (Yusuf, 2026-09-11).
   - **Changed** — the HTTP publisher's guard is name-based and nested for the same reason; no
     behaviour change for any consumer that has spring-web.
   - This is a real regression of the released 1.2.0 (present at the tag), so it IS a
     CHANGELOG item.
4. README: in "publisher SPI defaults", one sentence — "A consumer without Kafka on the
   classpath gets the HTTP publisher only; nothing Kafka-typed is linked."
5. Release-readiness per the standing rules: both `versions:display-property-updates` and
   `display-plugin-updates`, every profile (`mutation`, `release` — enumerate with
   `mvn help:all-profiles`), online, with the estate ignore string
   `(?i).*[.-](alpha|beta|m|rc|cr|dev|ea|preview)[0-9]*([.+-].*)?$|^[0-9a-f]{40}$`
   (the pom's own `<maven.version.ignore>` takes the same string). No image, so no Trivy.
6. `mvn release:prepare release:perform` (release profile via `releaseProfiles`, Central
   publishing); wait for Maven Central sync before step 7.
7. **opentmf-versions BOM 2.1.25** (as performed: 2.1.29, the BOM line current on the release day): `opentmf-outbox-service.version` 1.2.0 → 1.2.1
   (`pom.xml:73`); CR §1 floor note "at-next-touch, not a sweep".
8. Consumers: dnms-catalog drops `spring-kafka` and the `KafkaAutoConfiguration` exclusion at
   its next touch (its 1.2.0 CHANGELOG names both as the workaround); the six Kafka consumers
   pick 1.2.1 up through the BOM at their next touch — no behaviour change for them.

## 6. Not in scope

- Any change to the outbox schema, the relay, or the failure policy (1.2.0's surface stands).
- Making the relay scheduler virtual-thread aware (separate item on the opentmf list).
