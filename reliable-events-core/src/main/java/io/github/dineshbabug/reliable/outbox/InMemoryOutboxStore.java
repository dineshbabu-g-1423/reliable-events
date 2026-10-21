package io.github.dineshbabug.reliable.outbox;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Synchronised in-memory outbox for tests. */
public final class InMemoryOutboxStore implements OutboxStore {

    private static final class Row {
        final OutboxRecord base;
        int attempts;
        boolean published;
        Instant availableAt;
        String leasedBy;
        String lastError;

        Row(OutboxRecord base) {
            this.base = base;
            this.availableAt = base.createdAt();
        }
    }

    private final Map<Long, Row> rows = new LinkedHashMap<>();
    private final Clock clock;
    private long nextId = 1;

    public InMemoryOutboxStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public synchronized void append(String topic, String key, String eventId, byte[] payload) {
        long id = nextId++;
        rows.put(id, new Row(new OutboxRecord(id, topic, key, eventId, payload, clock.instant(), 0)));
    }

    @Override
    public synchronized List<OutboxRecord> claimBatch(int max, String workerId, Duration lease) {
        Instant now = clock.instant();
        List<OutboxRecord> claimed = new ArrayList<>();
        rows.values().stream()
                .filter(r -> !r.published && !r.availableAt.isAfter(now))
                .sorted(Comparator.comparingLong(r -> r.base.id()))
                .limit(max)
                .forEach(r -> {
                    r.leasedBy = workerId;
                    r.availableAt = now.plus(lease);
                    OutboxRecord b = r.base;
                    claimed.add(new OutboxRecord(b.id(), b.topic(), b.key(), b.eventId(), b.payload(), b.createdAt(), r.attempts));
                });
        return claimed;
    }

    @Override
    public synchronized void markPublished(long id) {
        Row row = rows.get(id);
        if (row != null) {
            row.published = true;
            row.leasedBy = null;
        }
    }

    @Override
    public synchronized void markFailed(long id, String error, Instant nextAttemptAt) {
        Row row = rows.get(id);
        if (row != null) {
            row.attempts++;
            row.lastError = error;
            row.availableAt = nextAttemptAt;
            row.leasedBy = null;
        }
    }

    public synchronized long unpublishedCount() {
        return rows.values().stream().filter(r -> !r.published).count();
    }

    public synchronized String lastError(long id) {
        Row row = rows.get(id);
        return row == null ? null : row.lastError;
    }
}
