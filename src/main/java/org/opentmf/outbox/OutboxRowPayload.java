package org.opentmf.outbox;

/**
 * A row's sensitive text, on its own path ({@code GET /ops/outbox/{id}/payload}, 1.11.1): the
 * stored payload and {@code last_error}, the failure's message - for an HTTP 4xx or 5xx the
 * receiver's whole response body. Its own path lets a consumer's path rules give it a stricter role
 * than the row views ({@code opentmf.outbox.ops.sensitive-in-views}). The stored headers are not
 * served, here or anywhere.
 */
public record OutboxRowPayload(long id, String payload, String lastError) {}
