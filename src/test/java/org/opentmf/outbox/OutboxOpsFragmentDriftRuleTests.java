package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * The consumer's drift rule for the pasted fragment (README "OAS fragment"), as code - the shape a
 * consumer's drift test takes, and the proof that the markers let every error model paste the paths
 * verbatim:
 *
 * <ul>
 *   <li>every fragment path is in the consumer's OAS, EQUAL;
 *   <li>a component marked {@code x-opentmf-refinable: true} exists under its name (the consumer's
 *       own answer, redefinable);
 *   <li>a component marked {@code x-opentmf-refinable: content} (1.8.0) exists under its name with
 *       the library's {@code description} and {@code headers}; its {@code content} is the
 *       consumer's error rendering;
 *   <li>every other component is EQUAL.
 * </ul>
 */
class OutboxOpsFragmentDriftRuleTests {

  private static final String MARKER = "x-opentmf-refinable";

  @SuppressWarnings("unchecked")
  private static Map<String, Object> fragment() {
    return new Yaml().load(OutboxOpsOpenApi.fragment());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Map<String, Object>> section(Map<String, Object> doc, String key) {
    Object value = doc.get(key);
    return value == null ? Map.of() : (Map<String, Map<String, Object>>) value;
  }

  /** The drift rule: what in {@code consumer} departs from the pasted {@code fragment}. */
  @SuppressWarnings("unchecked")
  static List<String> violations(Map<String, Object> consumer, Map<String, Object> fragment) {
    List<String> found = new ArrayList<>();
    section(fragment, "paths")
        .forEach(
            (path, operations) -> {
              if (!operations.equals(section(consumer, "paths").get(path))) {
                found.add("paths." + path);
              }
            });
    Map<String, Map<String, Object>> mine = section(consumer, "components");
    section(fragment, "components")
        .forEach(
            (kind, definitions) ->
                definitions.forEach(
                    (name, definition) -> {
                      Map<String, Object> theirs = (Map<String, Object>) definition;
                      Map<String, Object> ours =
                          mine.get(kind) == null
                              ? null
                              : (Map<String, Object>) mine.get(kind).get(name);
                      Object marker = theirs.get(MARKER);
                      boolean ok;
                      if (ours == null) {
                        ok = false;
                      } else if (Boolean.TRUE.equals(marker)) {
                        ok = true; // the consumer's own answer, redefined at will
                      } else if ("content".equals(marker)) {
                        ok =
                            theirs.get("description").equals(ours.get("description"))
                                && Objects.equals(theirs.get("headers"), ours.get("headers"))
                                && ours.containsKey("content");
                      } else {
                        ok = theirs.equals(ours);
                      }
                      if (!ok) {
                        found.add("components." + kind + "." + name);
                      }
                    }));
    return found;
  }

  /** A deep copy through YAML, so a test can edit the "consumer's" document freely. */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> copy(Map<String, Object> doc) {
    Yaml yaml = new Yaml();
    return yaml.load(yaml.dump(doc));
  }

  @Test
  void aVerbatimPaste_passes() {
    assertThat(violations(fragment(), fragment())).isEmpty();
  }

  /**
   * A consumer whose single advice answers EVERY error as {@code application/json} in its own
   * TMF-style shape - no problem+json anywhere: it redefines the consumer answers and {@code
   * OutboxProblem}, replaces the content of every library refusal, and pastes the paths as they
   * are.
   */
  @Test
  @SuppressWarnings("unchecked")
  void aConsumerRenderingEveryErrorAsItsOwnJson_pastesThePathsVerbatim() {
    Map<String, Object> consumer = copy(fragment());
    Map<String, Map<String, Object>> components = section(consumer, "components");
    Map<String, Object> ownError = Map.of("$ref", "#/components/schemas/Error");
    Map<String, Object> asJson = Map.of("application/json", Map.of("schema", ownError));
    components.get("schemas").put("OutboxProblem", ownError);
    components.get("schemas").put("Error", Map.of("type", "object"));
    components
        .get("responses")
        .replaceAll(
            (name, definition) -> {
              Map<String, Object> response = new LinkedHashMap<>((Map<String, Object>) definition);
              Object marker = response.get(MARKER);
              if (Boolean.TRUE.equals(marker)) {
                return Map.of("description", "the consumer's own wording", "content", asJson);
              }
              if ("content".equals(marker)) {
                response.put("content", asJson);
              }
              return response;
            });

    assertThat(violations(consumer, fragment())).isEmpty();
    assertThat(consumer.get("paths")).isEqualTo(fragment().get("paths")); // not one path edited
    assertThat(new Yaml().dump(consumer)) // and its document promises no RFC 9457 anywhere
        .doesNotContain("application/problem+json");
  }

  @Test
  @SuppressWarnings("unchecked")
  void aConsumerThatRewordsALibraryRefusal_orEditsAPath_drifts() {
    Map<String, Object> consumer = copy(fragment());
    Map<String, Object> notFound =
        (Map<String, Object>)
            section(consumer, "components").get("responses").get("outboxRowNotFound");
    notFound.put("description", "gone");
    Map<String, Object> inspect =
        (Map<String, Object>) section(consumer, "paths").get("/ops/outbox/{id}").get("get");
    ((Map<String, Object>) inspect.get("responses")).put("418", Map.of("description", "teapot"));

    assertThat(violations(consumer, fragment()))
        .containsExactlyInAnyOrder(
            "paths./ops/outbox/{id}", "components.responses.outboxRowNotFound");
  }
}
