package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Test;

/** The shipped seal rule: a clean codebase passes, a consumer-owned outbox repository fails. */
class OutboxArchRulesTests {

  @Test
  void theLibraryItself_neverViolatesTheSeal() {
    JavaClasses classes =
        new ClassFileImporter()
            .withImportOption(location -> !location.contains("fixture"))
            .importPackages("org.opentmf.outbox");
    assertThat(OutboxArchRules.consumersUseOnlyTheSeams().evaluate(classes).hasViolation())
        .isFalse();
  }

  @Test
  void aConsumerOwnedRepositoryOverTheOutboxEntity_isCaught() {
    JavaClasses classes =
        new ClassFileImporter()
            .importPackages("org.opentmf.outbox", "org.opentmf.outbox.fixture");
    assertThat(OutboxArchRules.consumersUseOnlyTheSeams().evaluate(classes).hasViolation())
        .isTrue();
  }
}
