package org.opentmf.outbox;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * The {@code /ops/outbox} wire contract as ONE OpenAPI fragment, shipped in the jar so every
 * consumer documents the surface the same way (paths, TMF630 paging with 200/206/416 and the
 * range headers, the toolkit's own error object) instead of hand-copying it four ways.
 *
 * <p>A consumer pastes the fragment's {@code paths} and {@code components} into its own OAS and
 * pins itself with a drift test: parse {@link #fragment()}, parse its own document, and assert
 * the {@code /ops/outbox} subtree is equal (README "OAS fragment"). The library's own guard keeps
 * the fragment equal to {@code OutboxOpsController}'s mappings.
 */
public final class OutboxOpsOpenApi {

  /** Classpath location of the fragment. */
  public static final String RESOURCE = "META-INF/openapi/opentmf-outbox-ops.oas.yaml";

  private OutboxOpsOpenApi() {}

  /** The fragment's YAML text, verbatim. */
  public static String fragment() {
    try (InputStream in = OutboxOpsOpenApi.class.getClassLoader().getResourceAsStream(RESOURCE)) {
      if (in == null) {
        throw new IllegalStateException("opentmf-outbox-service jar lacks " + RESOURCE);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }
}
