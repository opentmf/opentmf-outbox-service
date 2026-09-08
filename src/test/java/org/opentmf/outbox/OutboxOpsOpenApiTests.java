package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.internal.OutboxOpsController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.yaml.snakeyaml.Yaml;

/**
 * The shipped OAS fragment and the controller describe the SAME surface: every mapping of {@link
 * OutboxOpsController} is a fragment path with that method, and the fragment names nothing the
 * controller does not serve. A route added to one side without the other is a red build here,
 * not a consumer's discovery.
 */
class OutboxOpsOpenApiTests {

  @SuppressWarnings("unchecked")
  private static Map<String, Object> fragment() {
    return new Yaml().load(OutboxOpsOpenApi.fragment());
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
        ops.add("GET " + base + get.path()[0]);
      }
      if (post != null) {
        ops.add("POST " + base + post.path()[0]);
      }
    }
    return ops;
  }

  @Test
  @DisplayName("the fragment's operations are exactly the controller's mappings")
  void fragmentMatchesTheController() {
    assertThat(fragmentOperations()).containsExactlyElementsOf(controllerOperations());
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
}
