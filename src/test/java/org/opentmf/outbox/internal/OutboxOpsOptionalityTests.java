package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxMaintenanceService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;

/**
 * Proves the {@code tmf630-toolkit-all} dependency is REALLY optional — not merely labelled so.
 *
 * <p>The hazard is a quiet one: annotation classes absent at runtime are silently skipped by the
 * JVM, so without the toolkit the controller would still load and register — and the list
 * endpoint would serve WITHOUT its TMF630 rendering, a silent degradation no log line names.
 * The {@code @ConditionalOnClass} guard on the bean method converts that into a clean absence:
 * no toolkit, no {@code /ops} controller, and the README says why.
 *
 * <p>This test binds to the REAL bean method's annotation — a copy of the guard in a test
 * fixture would keep passing after someone edits the original (the copied-guard trap).
 */
class OutboxOpsOptionalityTests {

  @Test
  void theRealOpsBeanGuard_namesBothTheWebAndTheToolkitClasses() throws Exception {
    Method bean =
        OutboxAutoConfiguration.class.getMethod("outboxOpsController", OutboxMaintenanceService.class);
    ConditionalOnClass guard = bean.getAnnotation(ConditionalOnClass.class);

    assertThat(guard).as("the /ops bean must be class-guarded").isNotNull();
    assertThat(guard.name())
        .as("web alone is not enough — every web app has RestController; the toolkit is the"
            + " condition that makes the optional dependency honest")
        .contains(
            "org.springframework.web.bind.annotation.RestController",
            "org.opentmf.query.tmf630.annotation.Tmf630Response");
    assertThat(guard.value())
        .as("class-typed conditions would link the guarded classes from the auto-configuration"
            + " itself — the guard must stay name-based")
        .isEmpty();
  }
}
