package io.github.dineshbabug.reliable.outbox;

import java.time.Instant;
import java.util.Objects;

/**
 * An event written in the same database transaction as the business change that produced it.
 * A relay publishes it afterwards, so the event is never lost and never sent for a change that
 * rolled back.
 */
public record OutboxRecord(
        long id,
        String topic,
        String key,
        String eventId,
        byte[] payload,
        Instant createdAt,
        int attempts) {

    public OutboxRecord {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(createdAt, "createdAt");
        payload = payload.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
