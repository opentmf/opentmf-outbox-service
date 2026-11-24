package org.opentmf.outbox.internal;

import java.sql.SQLException;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The start-time check that the library's newest changeset has been applied (1.12.0, changeset
 * {@value #CHANGESET}): one {@code select key_token from outbox limit 0}, before the JPA
 * EntityManagerFactory starts and after the application's database initialisation - Liquibase,
 * which applies the included changelog's missing changesets at that very start.
 *
 * <p>A consumer that includes {@code db/changelog/opentmf-outbox.sql} by reference never meets it.
 * A deployment that runs the library's changelog OUT OF BAND and starts the new version on the old
 * schema does: every read of the table and every booking would fail - the column is mapped - so the
 * start is refused with ONE error naming the changeset and its statement, instead of a relay that
 * fails every pass. Any other failure of the probe (no table, another schema set-up, a database
 * that does not answer) is only logged: the probe never refuses a start that the rest of the
 * application would make.
 */
@Slf4j
final class OutboxSchemaCheck {

  /** The changeset whose column the check probes. */
  static final String CHANGESET = "009-outbox-key-token";

  /** The changeset's statement - what a deployment that runs the changelog out of band applies. */
  static final String STATEMENT = "alter table outbox add column if not exists key_token uuid;";

  /** PostgreSQL's SQLSTATE for {@code undefined_column}. */
  private static final String UNDEFINED_COLUMN = "42703";

  private OutboxSchemaCheck() {}

  /** Checks the outbox table behind {@code dataSource}; refuses the start if 009 is missing. */
  static OutboxSchemaCheck check(DataSource dataSource) {
    try {
      new JdbcTemplate(dataSource).execute("select key_token from outbox limit 0");
    } catch (DataAccessException ex) {
      if (undefinedColumn(ex)) {
        throw new IllegalStateException(
            ("The outbox table has no column key_token: the opentmf-outbox changeset %s has not"
                    + " been applied. Include db/changelog/opentmf-outbox.sql by reference in the"
                    + " application's Liquibase changelog (it applies the changeset at start), or,"
                    + " where the changelog runs out of band, apply it before this version starts:"
                    + " %s")
                .formatted(CHANGESET, STATEMENT),
            ex);
      }
      log.warn(
          "Outbox schema check not run - the probe of changeset {} failed for another reason;"
              + " the start goes on",
          CHANGESET,
          ex);
    }
    return new OutboxSchemaCheck();
  }

  private static boolean undefinedColumn(DataAccessException ex) {
    return ex.getMostSpecificCause() instanceof SQLException sql
        && UNDEFINED_COLUMN.equals(sql.getSQLState());
  }
}
