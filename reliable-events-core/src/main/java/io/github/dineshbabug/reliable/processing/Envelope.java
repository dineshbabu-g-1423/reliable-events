package io.github.dineshbabug.reliable.processing;

import java.util.Map;
import java.util.Objects;

/**
 * A message as the processor sees it, independent of the broker.
 *
 * @param messageId stable identity used for de-duplication (for example an event id header)
 * @param key       partitioning key, may be null
 * @param payload   decoded payload
 * @param headers   broker headers as strings
 */
public record Envelope<T>(String messageId, String key, T payload, Map<String, String> headers) {

    public Envelope {
        Objects.requireNonNull(messageId, "messageId");
        if (messageId.isBlank()) {
            throw new IllegalArgumentException("messageId must not be blank");
        }
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
