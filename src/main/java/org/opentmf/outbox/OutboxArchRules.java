package org.opentmf.outbox;

import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;

/**
 * The library-provided SEAL rule: consumers assert with one line that their business code
 * touches the outbox only through the public seams ({@link OutboxWriter},
 * {@link OutboxMaintenanceService}, {@link OutboxEvent}/{@link OutboxRowView} and the SPI
 * types). Everything else is package-private already - this rule additionally catches
 * reflection-free MISUSE such as a consumer's own JPA repository over the outbox table.
 *
 * <p>Usage in a consumer's ArchUnit suite:
 * {@code OutboxArchRules.consumersUseOnlyTheSeams().check(importedClasses);}
 */
public final class OutboxArchRules {

  private OutboxArchRules() {}

  /**
   * No consumer class declares its own persistence over the outbox table. The library's own
   * repository is package-private (unnameable outside), so the reachable misuse is a consumer
   * Spring Data repository over the PUBLIC {@link OutboxEvent} entity - that is what this rule
   * forbids. {@code allowEmptyShould}: a clean consumer has no such repositories at all.
   */
  public static ArchRule consumersUseOnlyTheSeams() {
    return ArchRuleDefinition.noClasses()
        .that()
        .resideOutsideOfPackage("org.opentmf.outbox")
        .and()
        .areAssignableTo(org.springframework.data.repository.Repository.class)
        .should()
        .dependOnClassesThat()
        .haveFullyQualifiedName("org.opentmf.outbox.OutboxEvent")
        .because(
            "the outbox table is library-owned (S23): business code appends through"
                + " OutboxWriter and operates through OutboxMaintenanceService, never through"
                + " its own persistence over the outbox table")
        .allowEmptyShould(true);
  }
}
