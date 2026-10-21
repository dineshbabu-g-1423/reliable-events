package io.github.dineshbabug.reliable.idempotency;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Thread-safe in-memory store for tests and single-instance use. Not durable across restarts. */
public final class InMemoryIdempotencyStore implements IdempotencyStore {

    private record Key(String consumerGroup, String messageId) {}

    private record Entry(boolean completed, Instant leaseUntil) {}

    private final ConcurrentMap<Key, Entry> entries = new ConcurrentHashMap<>();
    private final Clock clock;

    public InMemoryIdempotencyStore() {
        this(Clock.systemUTC());
    }

    public InMemoryIdempotencyStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public ClaimResult tryClaim(String consumerGroup, String messageId, Duration lease) {
        Objects.requireNonNull(lease, "lease");
        Key key = new Key(consumerGroup, messageId);
        Instant now = clock.instant();
        ClaimResult[] result = new ClaimResult[1];
        entries.compute(key, (k, existing) -> {
            if (existing == null || (!existing.completed() && !existing.leaseUntil().isAfter(now))) {
                result[0] = ClaimResult.CLAIMED;
                return new Entry(false, now.plus(lease));
            }
            result[0] = existing.completed() ? ClaimResult.ALREADY_COMPLETED : ClaimResult.IN_PROGRESS;
            return existing;
        });
        return result[0];
    }

    @Override
    public void markCompleted(String consumerGroup, String messageId) {
        entries.put(new Key(consumerGroup, messageId), new Entry(true, Instant.MAX));
    }

    @Override
    public void release(String consumerGroup, String messageId) {
        entries.computeIfPresent(new Key(consumerGroup, messageId),
                (k, existing) -> existing.completed() ? existing : null);
    }
}
