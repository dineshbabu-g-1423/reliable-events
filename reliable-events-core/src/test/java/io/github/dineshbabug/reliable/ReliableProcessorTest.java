package io.github.dineshbabug.reliable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dineshbabug.reliable.idempotency.InMemoryIdempotencyStore;
import io.github.dineshbabug.reliable.processing.Envelope;
import io.github.dineshbabug.reliable.processing.Outcome;
import io.github.dineshbabug.reliable.processing.ReliableProcessor;
import io.github.dineshbabug.reliable.retry.FailureClassifier;
import io.github.dineshbabug.reliable.retry.RetryPolicy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ReliableProcessorTest {

    private final InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
    private final List<Duration> sleeps = new ArrayList<>();
    private final List<String> deadLettered = new ArrayList<>();

    private ReliableProcessor<String> processor(io.github.dineshbabug.reliable.processing.MessageHandler<String> handler,
                                                FailureClassifier classifier) {
        return ReliableProcessor.builder("orders", handler)
                .idempotencyStore(store)
                .retryPolicy(new RetryPolicy(3, Duration.ofMillis(10), Duration.ofMillis(100)))
                .failureClassifier(classifier)
                .deadLetterPublisher((env, cause, attempts) -> deadLettered.add(env.messageId() + ":" + attempts))
                .sleeper(sleeps::add)
                .random(new SplittableRandom(42))
                .build();
    }

    private static Envelope<String> msg(String id) {
        return new Envelope<>(id, "k", "payload", Map.of());
    }

    @Test
    void processesOnceAndSkipsRedelivery() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        ReliableProcessor<String> p = processor(e -> calls.incrementAndGet(), f -> true);

        assertThat(p.process(msg("m1"))).isEqualTo(Outcome.PROCESSED);
        assertThat(p.process(msg("m1"))).isEqualTo(Outcome.DUPLICATE_SKIPPED);
        assertThat(calls).hasValue(1);
    }

    @Test
    void retriesTransientFailuresThenSucceeds() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        ReliableProcessor<String> p = processor(e -> {
            if (calls.incrementAndGet() < 3) {
                throw new IllegalStateException("downstream timeout");
            }
        }, f -> true);

        assertThat(p.process(msg("m1"))).isEqualTo(Outcome.PROCESSED);
        assertThat(calls).hasValue(3);
        assertThat(sleeps).hasSize(2);
        assertThat(deadLettered).isEmpty();
    }

    @Test
    void deadLettersAfterRetriesAreExhaustedAndDoesNotLoopOnRedelivery() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        ReliableProcessor<String> p = processor(e -> {
            calls.incrementAndGet();
            throw new IllegalStateException("always fails");
        }, f -> true);

        assertThat(p.process(msg("m1"))).isEqualTo(Outcome.DEAD_LETTERED);
        assertThat(deadLettered).containsExactly("m1:3");
        assertThat(p.process(msg("m1"))).isEqualTo(Outcome.DUPLICATE_SKIPPED);
        assertThat(calls).hasValue(3);
    }

    @Test
    void nonRetryableFailureGoesStraightToDeadLetter() throws Exception {
        ReliableProcessor<String> p = processor(e -> {
            throw new IllegalArgumentException("malformed payload");
        }, FailureClassifier.retryAllExcept(Set.of(IllegalArgumentException.class)));

        assertThat(p.process(msg("m1"))).isEqualTo(Outcome.DEAD_LETTERED);
        assertThat(deadLettered).containsExactly("m1:1");
        assertThat(sleeps).isEmpty();
    }

    @Test
    void failedDeadLetterReleasesTheClaimSoTheBrokerCanRedeliver() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        ReliableProcessor<String> p = ReliableProcessor.<String>builder("orders", e -> {
                    calls.incrementAndGet();
                    throw new IllegalArgumentException("bad");
                })
                .idempotencyStore(store)
                .retryPolicy(RetryPolicy.noRetries())
                .deadLetterPublisher((env, cause, attempts) -> { throw new IllegalStateException("dlq down"); })
                .sleeper(sleeps::add)
                .build();

        assertThatThrownBy(() -> p.process(msg("m1"))).hasMessage("dlq down");
        assertThatThrownBy(() -> p.process(msg("m1"))).hasMessage("dlq down");
        assertThat(calls).hasValue(2);
    }
}
