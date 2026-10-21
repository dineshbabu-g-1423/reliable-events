package io.github.dineshbabug.reliable.outbox;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** Storage for outbox records. A JDBC implementation lives in the Spring module. */
public interface OutboxStore {

    /** Append a record. Call inside the same transaction as the business change. */
    void append(String topic, String key, String eventId, byte[] payload);

    /**
     * Claim up to {@code max} unpublished records that are due, leasing them to {@code workerId}
     * for {@code lease}. Records leased by a worker that died become claimable again after the lease.
     */
    List<OutboxRecord> claimBatch(int max, String workerId, Duration lease);

    void markPublished(long id);

    /** Record a failed publish attempt and when the record may be tried again. */
    void markFailed(long id, String error, Instant nextAttemptAt);
}
