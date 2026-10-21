package io.github.dineshbabug.reliable;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dineshbabug.reliable.idempotency.ClaimResult;
import io.github.dineshbabug.reliable.idempotency.InMemoryIdempotencyStore;
import io.github.dineshbabug.reliable.support.MutableClock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class InMemoryIdempotencyStoreTest {

    private final MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    private final InMemoryIdempotencyStore store = new InMemoryIdempotencyStore(clock);
    private final Duration lease = Duration.ofSeconds(30);

    @Test
    void firstClaimWinsAndSecondSeesInProgress() {
        assertThat(store.tryClaim("g", "m1", lease)).isEqualTo(ClaimResult.CLAIMED);
        assertThat(store.tryClaim("g", "m1", lease)).isEqualTo(ClaimResult.IN_PROGRESS);
    }

    @Test
    void completedMessageIsReportedAsDuplicate() {
        store.tryClaim("g", "m1", lease);
        store.markCompleted("g", "m1");
        assertThat(store.tryClaim("g", "m1", lease)).isEqualTo(ClaimResult.ALREADY_COMPLETED);
    }

    @Test
    void expiredLeaseCanBeReclaimedAfterACrash() {
        store.tryClaim("g", "m1", lease);
        clock.advance(lease.plusSeconds(1));
        assertThat(store.tryClaim("g", "m1", lease)).isEqualTo(ClaimResult.CLAIMED);
    }

    @Test
    void releaseAllowsAnImmediateRetry() {
        store.tryClaim("g", "m1", lease);
        store.release("g", "m1");
        assertThat(store.tryClaim("g", "m1", lease)).isEqualTo(ClaimResult.CLAIMED);
    }

    @Test
    void releaseNeverUndoesCompletion() {
        store.tryClaim("g", "m1", lease);
        store.markCompleted("g", "m1");
        store.release("g", "m1");
        assertThat(store.tryClaim("g", "m1", lease)).isEqualTo(ClaimResult.ALREADY_COMPLETED);
    }

    @Test
    void consumerGroupsAreIndependent() {
        store.tryClaim("billing", "m1", lease);
        store.markCompleted("billing", "m1");
        assertThat(store.tryClaim("search", "m1", lease)).isEqualTo(ClaimResult.CLAIMED);
    }
}
