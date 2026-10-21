package io.github.dineshbabug.reliable.processing;

import io.github.dineshbabug.reliable.idempotency.ClaimResult;
import io.github.dineshbabug.reliable.idempotency.IdempotencyStore;
import io.github.dineshbabug.reliable.retry.FailureClassifier;
import io.github.dineshbabug.reliable.retry.RetryPolicy;
import java.time.Duration;
import java.util.Objects;
import java.util.random.RandomGenerator;

/**
 * Wraps a {@link MessageHandler} with de-duplication, bounded retries and dead-lettering.
 *
 * <p>Flow per message: claim the message id; skip if already completed or held by another
 * worker; otherwise call the handler, retrying retryable failures with backoff; on success mark
 * completed; on final failure dead-letter it and mark it completed, so redelivery of a poison
 * message does not loop. If dead-lettering itself fails, the claim is released and the failure
 * is rethrown, so the broker redelivers rather than the message being lost.
 */
public final class ReliableProcessor<T> {

    private final String consumerGroup;
    private final MessageHandler<T> handler;
    private final IdempotencyStore idempotency;
    private final RetryPolicy retryPolicy;
    private final FailureClassifier classifier;
    private final DeadLetterPublisher<T> deadLetters;
    private final Duration claimLease;
    private final Sleeper sleeper;
    private final RandomGenerator random;

    private ReliableProcessor(Builder<T> b) {
        this.consumerGroup = Objects.requireNonNull(b.consumerGroup, "consumerGroup");
        this.handler = Objects.requireNonNull(b.handler, "handler");
        this.idempotency = Objects.requireNonNull(b.idempotency, "idempotency");
        this.retryPolicy = Objects.requireNonNull(b.retryPolicy, "retryPolicy");
        this.classifier = Objects.requireNonNull(b.classifier, "classifier");
        this.deadLetters = Objects.requireNonNull(b.deadLetters, "deadLetters");
        this.claimLease = Objects.requireNonNull(b.claimLease, "claimLease");
        this.sleeper = Objects.requireNonNull(b.sleeper, "sleeper");
        this.random = Objects.requireNonNull(b.random, "random");
    }

    public static <T> Builder<T> builder(String consumerGroup, MessageHandler<T> handler) {
        return new Builder<>(consumerGroup, handler);
    }

    public Outcome process(Envelope<T> envelope) throws InterruptedException {
        String id = envelope.messageId();
        ClaimResult claim = idempotency.tryClaim(consumerGroup, id, claimLease);
        if (claim == ClaimResult.ALREADY_COMPLETED) {
            return Outcome.DUPLICATE_SKIPPED;
        }
        if (claim == ClaimResult.IN_PROGRESS) {
            return Outcome.IN_PROGRESS_SKIPPED;
        }

        int attempts = 0;
        while (true) {
            attempts++;
            try {
                handler.handle(envelope);
                idempotency.markCompleted(consumerGroup, id);
                return Outcome.PROCESSED;
            } catch (InterruptedException e) {
                idempotency.release(consumerGroup, id);
                throw e;
            } catch (Exception failure) {
                if (classifier.isRetryable(failure) && retryPolicy.canRetry(attempts)) {
                    sleepOrRelease(retryPolicy.delayBeforeRetry(attempts, random), id);
                    continue;
                }
                deadLetter(envelope, failure, attempts);
                idempotency.markCompleted(consumerGroup, id);
                return Outcome.DEAD_LETTERED;
            }
        }
    }

    private void sleepOrRelease(Duration delay, String id) throws InterruptedException {
        try {
            sleeper.sleep(delay);
        } catch (InterruptedException e) {
            idempotency.release(consumerGroup, id);
            throw e;
        }
    }

    private void deadLetter(Envelope<T> envelope, Exception failure, int attempts) {
        try {
            deadLetters.publish(envelope, failure, attempts);
        } catch (RuntimeException dlqFailure) {
            idempotency.release(consumerGroup, envelope.messageId());
            dlqFailure.addSuppressed(failure);
            throw dlqFailure;
        }
    }

    public static final class Builder<T> {
        private final String consumerGroup;
        private final MessageHandler<T> handler;
        private IdempotencyStore idempotency;
        private RetryPolicy retryPolicy = new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(5));
        private FailureClassifier classifier = f -> true;
        private DeadLetterPublisher<T> deadLetters;
        private Duration claimLease = Duration.ofMinutes(5);
        private Sleeper sleeper = Sleeper.THREAD;
        private RandomGenerator random = RandomGenerator.getDefault();

        private Builder(String consumerGroup, MessageHandler<T> handler) {
            this.consumerGroup = consumerGroup;
            this.handler = handler;
        }

        public Builder<T> idempotencyStore(IdempotencyStore store) { this.idempotency = store; return this; }
        public Builder<T> retryPolicy(RetryPolicy policy) { this.retryPolicy = policy; return this; }
        public Builder<T> failureClassifier(FailureClassifier classifier) { this.classifier = classifier; return this; }
        public Builder<T> deadLetterPublisher(DeadLetterPublisher<T> publisher) { this.deadLetters = publisher; return this; }
        public Builder<T> claimLease(Duration lease) { this.claimLease = lease; return this; }
        public Builder<T> sleeper(Sleeper sleeper) { this.sleeper = sleeper; return this; }
        public Builder<T> random(RandomGenerator random) { this.random = random; return this; }

        public ReliableProcessor<T> build() {
            return new ReliableProcessor<>(this);
        }
    }
}
