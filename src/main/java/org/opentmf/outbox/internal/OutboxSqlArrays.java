package org.opentmf.outbox.internal;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

/** PostgreSQL array literals for the relay's set-based statements (passed as text, then cast). */
final class OutboxSqlArrays {

  /** An instant as the database's array literal takes it - every digit, never an elided zero. */
  private static final DateTimeFormatter MICROS =
      DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSSXXX");

  private OutboxSqlArrays() {}

  /** {@code {7,12}} */
  static String ids(List<Long> ids) {
    return ids.stream().map(String::valueOf).collect(Collectors.joining(",", "{", "}"));
  }

  /**
   * {@code {"2026-10-16T08:00:00.123456Z",...}} - each to the microsecond, as the column holds it.
   */
  static String instants(List<OffsetDateTime> instants) {
    return instants.stream()
        .map(instant -> '"' + MICROS.format(instant) + '"')
        .collect(Collectors.joining(",", "{", "}"));
  }
}
