package io.github.dineshbabug.reliable;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dineshbabug.reliable.outbox.InMemoryOutboxStore;
import io.github.dineshbabug.reliable.outbox.OutboxRecord;
import io.github.dineshbabug.reliable.outbox.OutboxRelay;
import io.github.dineshbabug.reliable.retry.RetryPolicy;
import io.github.dineshbabug.reliable.support.MutableClock;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class OutboxRelayTest {

    private final MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    private final InMemoryOutboxStore store = new InMemoryOutboxStore(clock);
    private final List<String> sent = new ArrayList<>();
    private final RetryPolicy retry = new RetryPolicy(10, Duration.ofSeconds(1), Duration.ofSeconds(30));

    private OutboxRelay relay(io.github.dineshbabug.reliable.outbox.EventPublisher publisher) {
        return new OutboxRelay(store, publisher, "relay-1", 100, Duration.ofSeconds(30), retry, clock,
                new SplittableRandom(7));
    }

    private void append(String eventId) {
        store.append("orders", "order-1", eventId, eventId.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void publishesInOrderAndMarksPublished() {
        append("e1");
        append("e2");

        OutboxRelay.PassResult pass = relay(r -> sent.add(r.eventId())).runOnce();

        assertThat(pass).isEqualTo(new OutboxRelay.PassResult(2, 2, 0));
        assertThat(sent).containsExactly("e1", "e2");
        assertThat(store.unpublishedCount()).isZero();
        assertThat(relay(r -> sent.add(r.eventId())).runOnce().claimed()).isZero();
    }

    @Test
    void failedPublishIsRetriedAfterBackoffNotImmediately() {
        append("e1");
        boolean[] brokerUp = {false};
        OutboxRelay relay = relay(r -> {
            if (!brokerUp[0]) throw new IllegalStateException("broker unavailable");
            sent.add(r.eventId());
        });

        assertThat(relay.runOnce().failed()).isEqualTo(1);
        assertThat(store.lastError(1)).contains("broker unavailable");

        brokerUp[0] = true;
        clock.advance(Duration.ofSeconds(1).plusMillis(1));
        assertThat(relay.runOnce().published()).isEqualTo(1);
        assertThat(sent).containsExactly("e1");
    }

    @Test
    void recordLeasedByACrashedRelayBecomesClaimableAfterTheLease() {
        append("e1");
        List<OutboxRecord> taken = store.claimBatch(10, "relay-crashed", Duration.ofSeconds(30));
        assertThat(taken).hasSize(1);

        assertThat(relay(r -> sent.add(r.eventId())).runOnce().claimed()).isZero();

        clock.advance(Duration.ofSeconds(31));
        assertThat(relay(r -> sent.add(r.eventId())).runOnce().published()).isEqualTo(1);
    }
}
