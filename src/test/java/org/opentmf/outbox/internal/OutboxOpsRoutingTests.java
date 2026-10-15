package org.opentmf.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.http.HttpHeaders.ALLOW;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.opentmf.outbox.OutboxMaintenanceService;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;

/**
 * OUTBOX-OPS-ID-PATH-DIGITS-1 (1.7.0): the row routes take an id of digits only, so a literal
 * sibling segment is never read as an id. Before, {@code GET /ops/outbox/unpark} reached the
 * inspect route and answered 400 ("not a whole number"), and {@code OPTIONS /ops/outbox/unpark}
 * advertised GET. Now the first is the 405 of a path that serves POST only, the second names
 * POST alone, and any other segment that is not digits matches no row route (404).
 */
class OutboxOpsRoutingTests {

  private final OutboxMaintenanceService maintenance = mock(OutboxMaintenanceService.class);
  private final MockMvc mvc =
      MockMvcBuilders.standaloneSetup(new OutboxOpsController(maintenance)).build();

  @Test
  void getUnpark_isNeverTheInspectRoute_butAMethodTheBulkUnparkDoesNotServe() throws Exception {
    mvc.perform(get("/ops/outbox/unpark"))
        .andExpect(status().isMethodNotAllowed())
        .andExpect(header().string(ALLOW, allOf(containsString("POST"), not(containsString("GET")))));

    verifyNoInteractions(maintenance);
  }

  @Test
  void optionsUnpark_advertisesWhatThePathServes_neverGet() throws Exception {
    mvc.perform(options("/ops/outbox/unpark"))
        .andExpect(status().isOk())
        .andExpect(header().string(ALLOW, allOf(containsString("POST"), not(containsString("GET")))));
  }

  @Test
  void aSegmentThatIsNotDigits_matchesNoRowRoute() throws Exception {
    mvc.perform(get("/ops/outbox/abc")).andExpect(status().isNotFound());
    mvc.perform(get("/ops/outbox/state")).andExpect(status().isNotFound());
    mvc.perform(get("/ops/outbox/-1")).andExpect(status().isNotFound());
    mvc.perform(post("/ops/outbox/abc/unpark")).andExpect(status().isNotFound());
    mvc.perform(post("/ops/outbox/parked/cancel")).andExpect(status().isNotFound());

    verifyNoInteractions(maintenance);
  }

  @Test
  void digits_areTheRowRoutes() throws Exception {
    mvc.perform(get("/ops/outbox/42")).andExpect(status().isOk());
    mvc.perform(post("/ops/outbox/42/unpark")).andExpect(status().isOk());
    mvc.perform(post("/ops/outbox/42/cancel")).andExpect(status().isOk());

    verify(maintenance).inspect(42L);
    verify(maintenance).unpark(42L);
    verify(maintenance).cancel(42L);
  }

  @Test
  void everyIdRoute_isConstrainedToDigits() {
    List<String> idRoutes = new ArrayList<>();
    for (Method m : OutboxOpsController.class.getDeclaredMethods()) {
      GetMapping get = m.getAnnotation(GetMapping.class);
      PostMapping post = m.getAnnotation(PostMapping.class);
      String path = get != null ? get.path()[0] : post != null ? post.path()[0] : "";
      if (path.contains("{id")) {
        idRoutes.add(path);
      }
    }

    assertThat(idRoutes).hasSize(3).allMatch(path -> path.contains("{id:\\d+}"));
  }
}
