package org.opentmf.outbox;

import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

/**
 * {@link OutboxHttpLaneIT} again with {@code spring.threads.virtual.enabled=true}: the CONCURRENT
 * lane's sends run on VIRTUAL threads. Runs only in the failsafe execution forked on a JDK 21+
 * toolchain (the pom's {@code virtual-threads-it}); on a pre-21 runtime it FAILS rather than
 * skips - "both tested" must never quietly mean one.
 */
// the superclass's nested @TestConfiguration is NOT detected for a subclass - import it
@Import(OutboxHttpLaneIT.Stub.class)
@TestPropertySource(properties = "spring.threads.virtual.enabled=true")
class OutboxHttpLaneVirtualIT extends OutboxHttpLaneIT {

  @Override
  boolean expectVirtualThreads() {
    return true;
  }
}
