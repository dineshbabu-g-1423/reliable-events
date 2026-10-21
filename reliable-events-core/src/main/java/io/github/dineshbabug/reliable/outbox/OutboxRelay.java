package io.github.dineshbabug.reliable.outbox;

import io.github.dineshbabug.reliable.retry.RetryPolicy;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.random.RandomGenerator;

/**
 * Moves outbox records to the broker. Run {@link #runOnce()} on a schedule from one or more
 * instances; leasing keeps two relays from publishing the same record at the same time.
 *
 * <p>Delivery is at-least-once: a relay can crash after the broker accepted a record but
 * before marking it published. Consumers de-duplicate on {@link OutboxRecord#eventId()}.
 */
public final class OutboxRelay {

    /** Counts from one pass, for logging and metrics. */
    public record PassResult(int claimed, int published, int failed) {}

    private final OutboxStore store;
    private final EventPublisher publisher;
    private final String workerId;
    private final int batchSize;
    private final Duration lease;
    private final RetryPolicy retryPolicy;
    private final Clock clock;
    private final RandomGenerator random;

    public OutboxRelay(OutboxStore store, EventPublisher publisher, String workerId, int batchSize,
                       Duration lease, RetryPolicy retryPolicy, Clock clock, RandomGenerator random) {
        this.store = Objects.requireNonNull(store, "store");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.workerId = Objects.requireNonNull(workerId, "workerId");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be at least 1");
        }
        this.batchSize = batchSize;
        this.lease = Objects.requireNonNull(lease, "lease");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = Objects.requireNonNull(random, "random");
    }

    public PassResult runOnce() {
        List<OutboxRecord> batch = store.claimBatch(batchSize, workerId, lease);
        int published = 0;
        int failed = 0;
        for (OutboxRecord record : batch) {
            try {
                publisher.publish(record);
                store.markPublished(record.id());
                published++;
            } catch (Exception e) {
                int retry = Math.max(1, record.attempts() + 1);
                store.markFailed(record.id(), describe(e),
                        clock.instant().plus(retryPolicy.delayBeforeRetry(retry, random)));
                failed++;
            }
        }
        return new PassResult(batch.size(), published, failed);
    }

    private static String describe(Exception e) {
        String message = e.getMessage();
        String text = e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
        return text.length() > 500 ? text.substring(0, 500) : text;
    }
}
