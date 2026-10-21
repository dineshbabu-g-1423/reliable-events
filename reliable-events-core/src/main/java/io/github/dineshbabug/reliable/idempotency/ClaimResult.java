package io.github.dineshbabug.reliable.idempotency;

/** Result of trying to claim a message for processing. */
public enum ClaimResult {
    /** This caller now owns the message and must process it. */
    CLAIMED,
    /** The message was already processed successfully; skip it. */
    ALREADY_COMPLETED,
    /** Another worker holds a live lease on the message; skip for now. */
    IN_PROGRESS
}
