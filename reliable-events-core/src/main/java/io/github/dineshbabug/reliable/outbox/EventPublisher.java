package io.github.dineshbabug.reliable.outbox;

/** Sends an outbox record to the broker. Must block until the broker acknowledges, or throw. */
@FunctionalInterface
public interface EventPublisher {
    void publish(OutboxRecord record) throws Exception;
}
