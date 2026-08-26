package org.opentmf.outbox;

import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.CompositeArchRule;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import org.springframework.data.repository.Repository;

/**
 * The library-provided SEAL rule: consumers assert with one line that their code touches the
 * outbox only through the public contract of {@code org.opentmf.outbox} - {@link OutboxWriter},
 * {@link OutboxMaintenanceService}, {@link OutboxEvent}/{@link OutboxRowView}, the state
 * vocabulary and the SPI types. Everything under {@code org.opentmf.outbox.internal} is an
 * implementation detail with no compatibility promise.
 *
 * <p>Usage in a consumer's ArchUnit suite:
 * {@code OutboxArchRules.consumersUseOnlyTheSeams().check(importedClasses);}
 */
public final class OutboxArchRules {

  /** The public contract package. */
  public static final String PUBLIC_PACKAGE = "org.opentmf.outbox";

  /** The implementation package - never referenced by consumer code. */
  public static final String INTERNAL_PACKAGE = PUBLIC_PACKAGE + ".internal";

  private OutboxArchRules() {}

  /**
   * Both halves of the seal: (1) no class outside the library depends on the internal package -
   * the boundary is package-shaped, so relay, repository, publishers, auto-configuration and
   * the ops controller are all covered structurally; (2) no consumer class declares its own
   * Spring Data persistence over the public {@link OutboxEvent} entity - the one misuse the
   * package boundary alone cannot see. {@code allowEmptyShould}: a clean consumer has no such
   * classes at all.
   */
  public static ArchRule consumersUseOnlyTheSeams() {
    return CompositeArchRule.of(noConsumerDependsOnInternals())
        .and(noConsumerRepositoryOverTheOutboxTable())
        .as("consumers use only the outbox seams");
  }

  private static ArchRule noConsumerDependsOnInternals() {
    return ArchRuleDefinition.noClasses()
        .that()
        .resideOutsideOfPackages(PUBLIC_PACKAGE, INTERNAL_PACKAGE)
        .should()
        .dependOnClassesThat()
        .resideInAPackage(INTERNAL_PACKAGE + "..")
        .because(
            "the outbox implementation is internal: business code appends through OutboxWriter"
                + " and operates through OutboxMaintenanceService")
        .allowEmptyShould(true);
  }

  private static ArchRule noConsumerRepositoryOverTheOutboxTable() {
    return ArchRuleDefinition.noClasses()
        .that()
        .resideOutsideOfPackages(PUBLIC_PACKAGE, INTERNAL_PACKAGE)
        .and()
        .areAssignableTo(Repository.class)
        .should()
        .dependOnClassesThat()
        .haveFullyQualifiedName(PUBLIC_PACKAGE + ".OutboxEvent")
        .because(
            "the outbox table is library-owned: business code never declares its own persistence"
                + " over it")
        .allowEmptyShould(true);
  }
}
