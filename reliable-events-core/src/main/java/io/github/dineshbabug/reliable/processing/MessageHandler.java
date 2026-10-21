package io.github.dineshbabug.reliable.processing;

/** Business logic for one message. Throw to signal failure. */
@FunctionalInterface
public interface MessageHandler<T> {
    void handle(Envelope<T> envelope) throws Exception;
}
