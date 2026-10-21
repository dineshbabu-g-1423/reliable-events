package io.github.dineshbabug.reliable.idempotency;

import java.time.Duration;

/**
 * Records which messages a consumer group has processed, so that at-least-once delivery
 * becomes effectively-once processing.
 *
 * <p>A claim carries a lease. If a worker crashes mid-processing, its lease expires and the
 * message can be claimed again on redelivery, instead of being skipped forever.
 */
public interface IdempotencyStore {

    /** Try to claim {@code messageId} for {@code consumerGroup}, holding it for {@code lease}. */
    ClaimResult tryClaim(String consumerGroup, String messageId, Duration lease);

    /** Mark a claimed message as processed. Later claims return {@link ClaimResult#ALREADY_COMPLETED}. */
    void markCompleted(String consumerGroup, String messageId);

    /** Give up a claim without completing it, so the message can be retried on redelivery. */
    void release(String consumerGroup, String messageId);
}
