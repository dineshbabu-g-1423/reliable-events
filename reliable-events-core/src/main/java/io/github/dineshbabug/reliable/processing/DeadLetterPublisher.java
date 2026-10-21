package io.github.dineshbabug.reliable.processing;

/** Parks a message that cannot be processed, with the reason, so it can be inspected and replayed. */
@FunctionalInterface
public interface DeadLetterPublisher<T> {
    void publish(Envelope<T> envelope, Throwable cause, int attempts);
}
