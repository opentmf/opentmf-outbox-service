package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.internal.OutboxOpsController;
import org.opentmf.query.tmf630.filtering.Tmf630PassThrough;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.yaml.snakeyaml.Yaml;

/**
 * The shipped OAS fragment and the controller describe the SAME surface: every mapping of {@link
 * OutboxOpsController} is a fragment path with that method, and the fragment names nothing the
 * controller does not serve. A route added to one side without the other is a red build here,
 * not a consumer's discovery. Since 1.7.0 it also pins what a VERBATIM paste needs to be complete:
 * every operation names the answers of the consumer's security and transport by reference, every
 * error carries a body, and the components the consumer may refine are marked - nothing else.
 */
class OutboxOpsOpenApiTests {

  /** The answers of the CONSUMER's security and transport, on every operation (1.7.0). */
  private static final Map<String, String> CONSUMER_ANSWERS =
      Map.of(
          "401", "outboxUnauthorized",
          "403", "outboxForbidden",
          "405", "outboxMethodNotAllowed",
          "406", "outboxNotAcceptable",
          "503", "outboxServiceUnavailable");

  /** The marker of a component the consumer may redefine under the same name. */
  private static final String REFINABLE = "x-opentmf-refinable";

  @SuppressWarnings("unchecked")
  private static Map<String, Object> fragment() {
    return new Yaml().load(OutboxOpsOpenApi.fragment());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> components(String kind) {
    return (Map<String, Object>) ((Map<String, Object>) fragment().get("components")).get(kind);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> component(String kind, String name) {
    return (Map<String, Object>) components(kind).get(name);
  }

  /** Every operation of the fragment, keyed "VERB /path". */
  @SuppressWarnings("unchecked")
  private static Map<String, Map<String, Object>> operations() {
    Map<String, Map<String, Object>> paths = (Map<String, Map<String, Object>>) fragment().get("paths");
    Map<String, Map<String, Object>> ops = new TreeMap<>();
    paths.forEach(
        (path, item) ->
            item.forEach((verb, op) -> ops.put(verb.toUpperCase() + " " + path, (Map<String, Object>) op)));
    return ops;
  }

  /** A response as the reader sees it: a {@code $ref} to a response component followed. */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> resolved(Object response) {
    Map<String, Object> map = (Map<String, Object>) response;
    Object ref = map.get("$ref");
    return ref == null ? map : component("responses", ref.toString().substring("#/components/responses/".length()));
  }

  private static void collectRefs(Object node, List<String> refs) {
    if (node instanceof Map<?, ?> map) {
      map.forEach(
          (key, value) -> {
            if ("$ref".equals(key)) {
              refs.add(value.toString());
            } else {
              collectRefs(value, refs);
            }
          });
    } else if (node instanceof List<?> list) {
      list.forEach(item -> collectRefs(item, refs));
    }
  }

  private static Set<String> fragmentOperations() {
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> paths = (Map<String, Map<String, Object>>) fragment().get("paths");
    Set<String> ops = new TreeSet<>();
    paths.forEach((path, item) -> item.keySet().forEach(verb -> ops.add(verb.toUpperCase() + " " + path)));
    return ops;
  }

  private static Set<String> controllerOperations() {
    String base = OutboxOpsController.class.getAnnotation(RequestMapping.class).value()[0];
    Set<String> ops = new TreeSet<>();
    for (Method m : OutboxOpsController.class.getDeclaredMethods()) {
      GetMapping get = m.getAnnotation(GetMapping.class);
      PostMapping post = m.getAnnotation(PostMapping.class);
      if (get != null) {
        ops.add("GET " + base + template(get.path()[0]));
      }
      if (post != null) {
        ops.add("POST " + base + template(post.path()[0]));
      }
    }
    return ops;
  }

  /** A mapping's path as OAS writes it: {@code {id:\d+}} is the path parameter {@code {id}}. */
  private static String template(String mapping) {
    return mapping.replaceAll("\\{(\\w+):[^}]*}", "{$1}");
  }

  @Test
  @DisplayName("the fragment's operations are exactly the controller's mappings")
  void fragmentMatchesTheController() {
    assertThat(fragmentOperations()).containsExactlyElementsOf(controllerOperations());
  }

  @Test
  @DisplayName("every name a handler passes through the TMF630 grammar is a documented query parameter")
  void passedThroughNamesAreDocumented() {
    String base = OutboxOpsController.class.getAnnotation(RequestMapping.class).value()[0];
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Map<String, Object>>> paths =
        (Map<String, Map<String, Map<String, Object>>>) fragment().get("paths");
    int checked = 0;
    for (Method m : OutboxOpsController.class.getDeclaredMethods()) {
      Tmf630PassThrough passThrough = m.getAnnotation(Tmf630PassThrough.class);
      if (passThrough == null) {
        continue;
      }
      String path = base + m.getAnnotation(GetMapping.class).path()[0];
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> parameters =
          (List<Map<String, Object>>) paths.get(path).get("get").get("parameters");
      Set<String> queryNames = new TreeSet<>();
      parameters.stream()
          .filter(p -> "query".equals(p.get("in")))
          .forEach(p -> queryNames.add(p.get("name").toString()));
      assertThat(queryNames).as(path).contains(passThrough.value());
      checked++;
    }
    assertThat(checked).isPositive(); // ?state= on GET /ops/outbox (1.4.0)
  }

  @Test
  @DisplayName("every list operation documents the TMF630 trio: 200, 206 and 416 with the range headers")
  void listOperationsCarryThePagingContract() {
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Map<String, Object>>> paths =
        (Map<String, Map<String, Map<String, Object>>>) fragment().get("paths");
    for (String list : Set.of("/ops/outbox", "/ops/outbox/state/{state}", "/ops/outbox/parked")) {
      @SuppressWarnings("unchecked")
      Map<String, Object> responses = (Map<String, Object>) paths.get(list).get("get").get("responses");
      assertThat(responses).as(list).containsKeys("200", "206", "400", "416");
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> headers =
        (Map<String, Object>) ((Map<String, Object>) fragment().get("components")).get("headers");
    assertThat(headers).containsKeys("X-Total-Count", "X-Result-Count", "Content-Range", "Link");
  }

  @Test
  @DisplayName("the fragment names the version of the library that ships it")
  void fragmentIsVersioned() {
    @SuppressWarnings("unchecked")
    Map<String, Object> info = (Map<String, Object>) fragment().get("info");
    assertThat(info.get("version").toString()).matches("\\d+\\.\\d+\\.\\d+");
    assertThat(OutboxOpsOpenApi.RESOURCE).endsWith(".oas.yaml");
  }

  @Test
  @DisplayName("the OutboxRow schema names exactly the fields OutboxRowView serves")
  void rowSchemaMatchesTheRowView() {
    @SuppressWarnings("unchecked")
    Map<String, Object> schemas =
        (Map<String, Object>) ((Map<String, Object>) fragment().get("components")).get("schemas");
    @SuppressWarnings("unchecked")
    Map<String, Object> row = (Map<String, Object>) schemas.get("OutboxRow");
    @SuppressWarnings("unchecked")
    Set<String> documented = new TreeSet<>(((Map<String, Object>) row.get("properties")).keySet());
    Set<String> served = new TreeSet<>();
    for (RecordComponent component : OutboxRowView.class.getRecordComponents()) {
      served.add(component.getName());
    }
    assertThat(documented).isEqualTo(served);
  }

  @Test
  @DisplayName("limit declares no maximum: the toolkit CLAMPS a larger value to max-limit, it never rejects it")
  void theLimitDeclaresNoMaximum_becauseALargerValueIsClamped() {
    @SuppressWarnings("unchecked")
    Map<String, Object> schema = (Map<String, Object>) component("parameters", "outboxLimit").get("schema");

    // OutboxRoundTripIT pins the behaviour: limit=99999 is a 200 with every row, not a 400
    assertThat(schema).doesNotContainKey("maximum").containsEntry("minimum", 1);
    assertThat(component("parameters", "outboxLimit").get("description").toString()).contains("CLAMPED");
  }

  @Test
  @DisplayName("the row id is digits only, as the {id:\\d+} routes take it")
  void theRowIdIsDigitsOnly() {
    @SuppressWarnings("unchecked")
    Map<String, Object> schema = (Map<String, Object>) component("parameters", "outboxRowId").get("schema");

    assertThat(schema).containsEntry("type", "integer").containsEntry("format", "int64").containsEntry("minimum", 0);
  }

  @Test
  @DisplayName("every operation names the consumer's security and transport answers by reference; 415 where a body is read")
  void everyOperationDocumentsTheConsumersAnswers() {
    operations()
        .forEach(
            (name, op) -> {
              @SuppressWarnings("unchecked")
              Map<String, Object> responses = (Map<String, Object>) op.get("responses");
              CONSUMER_ANSWERS.forEach(
                  (code, component) ->
                      assertThat(responses)
                          .as("%s %s", name, code)
                          .containsEntry(code, Map.of("$ref", "#/components/responses/" + component)));
              if (op.containsKey("requestBody")) {
                assertThat(responses)
                    .as("%s 415", name)
                    .containsEntry("415", Map.of("$ref", "#/components/responses/outboxUnsupportedMediaType"));
              } else {
                assertThat(responses).as("%s reads no body", name).doesNotContainKey("415");
              }
            });
  }

  @Test
  @DisplayName("every error response carries a body: no description-only 4xx or 5xx")
  void everyErrorResponseCarriesABody() {
    operations()
        .forEach(
            (name, op) -> {
              @SuppressWarnings("unchecked")
              Map<String, Object> responses = (Map<String, Object>) op.get("responses");
              responses.forEach(
                  (code, response) -> {
                    if (!code.startsWith("2")) {
                      assertThat(resolved(response)).as("%s %s", name, code).containsKey("content");
                    }
                  });
            });
  }

  @Test
  @DisplayName("the consumer's answers and the problem body are the refinable components - and nothing else is")
  void onlyTheConsumersAnswersAreRefinable() {
    Set<String> expected = new TreeSet<>();
    CONSUMER_ANSWERS.values().forEach(c -> expected.add("responses." + c));
    expected.add("responses.outboxUnsupportedMediaType");
    expected.add("schemas.OutboxProblem");

    Set<String> marked = new TreeSet<>();
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Map<String, Object>>> all =
        (Map<String, Map<String, Map<String, Object>>>) fragment().get("components");
    all.forEach(
        (kind, defs) ->
            defs.forEach(
                (name, def) -> {
                  if (Boolean.TRUE.equals(def.get(REFINABLE))) {
                    marked.add(kind + "." + name);
                  }
                }));
    assertThat(marked).isEqualTo(expected);

    for (String response : expected) {
      if (response.startsWith("responses.")) {
        assertThat(component("responses", response.substring("responses.".length())))
            .as(response)
            .containsEntry(
                "content",
                Map.of(
                    "application/problem+json",
                    Map.of("schema", Map.of("$ref", "#/components/schemas/OutboxProblem"))));
      }
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> problem = (Map<String, Object>) component("schemas", "OutboxProblem").get("properties");
    assertThat(problem).containsOnlyKeys("type", "title", "status", "detail", "instance");
  }

  @Test
  @DisplayName(
      "every error is a $ref to a marked component: true for the consumer's own answers, content"
          + " for the library's refusals the consumer renders")
  void everyErrorIsAMarkedComponent() {
    Set<String> wholly = new TreeSet<>(CONSUMER_ANSWERS.keySet());
    wholly.add("415");
    operations()
        .forEach(
            (name, op) -> {
              @SuppressWarnings("unchecked")
              Map<String, Object> responses = (Map<String, Object>) op.get("responses");
              responses.forEach(
                  (code, response) -> {
                    if (code.startsWith("2")) {
                      assertThat(resolved(response)).as("%s %s", name, code).doesNotContainKey(REFINABLE);
                      return;
                    }
                    // the paths stay verbatim for every consumer: an error is never inline
                    @SuppressWarnings("unchecked")
                    Map<String, Object> asWritten = (Map<String, Object>) response;
                    assertThat(asWritten).as("%s %s", name, code).containsOnlyKeys("$ref");
                    Object marker = resolved(response).get(REFINABLE);
                    assertThat(marker)
                        .as("%s %s is marked", name, code)
                        .isEqualTo(wholly.contains(code) ? (Object) Boolean.TRUE : "content");
                  });
            });
  }

  @Test
  @DisplayName("a content-refinable response names when it happens and documents a default rendering")
  void aContentRefinableResponse_keepsItsMeaning_andADefaultContent() {
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> responses =
        (Map<String, Map<String, Object>>) (Map<?, ?>) components("responses");
    Set<String> content = new TreeSet<>();
    responses.forEach(
        (name, response) -> {
          if ("content".equals(response.get(REFINABLE))) {
            content.add(name);
            assertThat(response.get("description").toString())
                .as("%s names when it happens, never a media type - that is the content's", name)
                .isNotBlank()
                .doesNotContain("application/");
            assertThat(response).as(name).containsKey("content");
          }
        });
    assertThat(content)
        .containsExactlyInAnyOrder(
            "outboxBadRequest",
            "outboxRangeNotSatisfiable",
            "outboxRowIdOutOfRange",
            "outboxRowNotFound",
            "outboxInspectBadRequest",
            "outboxUnparkConflict",
            "outboxCancelConflict",
            "outboxUnparkFilterBadRequest");
    // the marker has exactly two values
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Map<String, Object>>> all =
        (Map<String, Map<String, Map<String, Object>>>) fragment().get("components");
    all.values().stream()
        .flatMap(defs -> defs.values().stream())
        .map(def -> def.get(REFINABLE))
        .filter(Objects::nonNull)
        .forEach(marker -> assertThat(marker).isIn(Boolean.TRUE, "content"));
  }

  @Test
  @DisplayName("every $ref of the fragment resolves inside the fragment")
  void everyReferenceResolves() {
    List<String> refs = new ArrayList<>();
    collectRefs(fragment(), refs);

    assertThat(refs).isNotEmpty();
    for (String ref : refs) {
      String[] parts = ref.split("/");
      assertThat(parts).as(ref).hasSize(4);
      assertThat(parts[0] + "/" + parts[1]).as(ref).isEqualTo("#/components");
      assertThat(components(parts[2])).as(ref).containsKey(parts[3]);
    }
  }
}
