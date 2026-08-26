package org.opentmf.outbox;

import org.springframework.web.client.RestClient;

/**
 * Consumer SPI for the HTTP publisher's named-client selection: a subscriber requiring
 * authentication is onboarded under a named client profile; delivery may select it per row, or
 * resolve it by base-url longest-prefix. The library ships no default
 * beyond plain POST - a consumer wanting authenticated deliveries contributes ONE bean that
 * maps its configured profiles to ready {@link RestClient}s.
 */
public interface OutboxClientProfileResolver {

  /**
   * The client for a named profile, or the best client for a destination URL when no profile is
   * set (longest-prefix base-url match). Return null for "no special client"
   * - the publisher then uses its plain default.
   *
   * @param clientProfile the row's explicit profile, possibly null
   * @param destination the row's destination URL (for base-url matching when profile is null)
   */
  RestClient resolve(String clientProfile, String destination);
}
