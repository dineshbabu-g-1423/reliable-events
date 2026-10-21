package io.github.dineshbabug.reliable.processing;

/** What happened to a message. */
public enum Outcome {
    /** The handler succeeded. */
    PROCESSED,
    /** The message had already been processed; the handler was not called. */
    DUPLICATE_SKIPPED,
    /** Another worker is processing it right now; the handler was not called. */
    IN_PROGRESS_SKIPPED,
    /** Retries were exhausted or the failure was not retryable; the message was dead-lettered. */
    DEAD_LETTERED
}
