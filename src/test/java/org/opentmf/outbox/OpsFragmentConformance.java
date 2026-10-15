package org.opentmf.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.RequestBuilder;
import org.yaml.snakeyaml.Yaml;

/**
 * A black-box suite's check in miniature: a request answers the expected status, the shipped
 * fragment documents that status on the operation, and the documented response names the media type
 * the answer is written in. A drift between the fragment and the wire is a red build here, not an
 * adopter's waiver.
 */
public final class OpsFragmentConformance {

  private OpsFragmentConformance() {}

  /**
   * Performs {@code request}, expects {@code status}, and asserts the fragment documents it on
   * {@code operation} ("VERB /path" as the fragment writes it) with the written media type.
   *
   * @return the media type the answer was written in, without parameters
   */
  public static MediaType answers(MockMvc mvc, String operation, RequestBuilder request, int status)
      throws Exception {
    MvcResult result = mvc.perform(request).andExpect(status().is(status)).andReturn();
    String contentType = result.getResponse().getContentType();
    assertThat(contentType).as("%s %d carries a body", operation, status).isNotNull();
    MediaType written = MediaType.parseMediaType(contentType);

    @SuppressWarnings("unchecked")
    Map<String, Object> content =
        (Map<String, Object>) documented(operation, status).get("content");

    MediaType bare = new MediaType(written.getType(), written.getSubtype());
    assertThat(content)
        .as("%s %d documented as %s", operation, status, bare)
        .containsKey(bare.toString());
    return bare;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> documented(String operation, int status) {
    String[] verbAndPath = operation.split(" ", 2);
    Map<String, Object> fragment = new Yaml().load(OutboxOpsOpenApi.fragment());
    Map<String, Object> path =
        ((Map<String, Map<String, Object>>) fragment.get("paths")).get(verbAndPath[1]);
    assertThat(path).as("the fragment has %s", verbAndPath[1]).isNotNull();
    Map<String, Object> op = (Map<String, Object>) path.get(verbAndPath[0].toLowerCase());
    Map<String, Object> response =
        (Map<String, Object>)
            ((Map<String, Object>) op.get("responses")).get(String.valueOf(status));
    assertThat(response).as("%s documents %d", operation, status).isNotNull();
    Object ref = response.get("$ref");
    if (ref != null) {
      Map<String, Object> components = (Map<String, Object>) fragment.get("components");
      response =
          ((Map<String, Map<String, Object>>) components.get("responses"))
              .get(ref.toString().substring("#/components/responses/".length()));
    }
    return response;
  }
}
