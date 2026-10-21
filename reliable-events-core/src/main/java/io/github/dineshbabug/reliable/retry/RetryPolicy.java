package io.github.dineshbabug.reliable.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.random.RandomGenerator;

/**
 * Bounded retries with capped exponential backoff and full jitter.
 *
 * <p>Full jitter (a random delay between zero and the capped exponential value) spreads
 * retries out, so many consumers failing at once do not retry in lockstep.
 *
 * @param maxAttempts total attempts including the first, at least 1
 * @param baseDelay   delay ceiling for the first retry
 * @param maxDelay    upper bound for any single delay
 */
public record RetryPolicy(int maxAttempts, Duration baseDelay, Duration maxDelay) {

    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        Objects.requireNonNull(baseDelay, "baseDelay");
        Objects.requireNonNull(maxDelay, "maxDelay");
        if (baseDelay.isNegative() || maxDelay.compareTo(baseDelay) < 0) {
            throw new IllegalArgumentException("require 0 <= baseDelay <= maxDelay");
        }
    }

    /** A single attempt, no retries. */
    public static RetryPolicy noRetries() {
        return new RetryPolicy(1, Duration.ZERO, Duration.ZERO);
    }

    /** True if another attempt is allowed after {@code attemptsSoFar} attempts. */
    public boolean canRetry(int attemptsSoFar) {
        return attemptsSoFar < maxAttempts;
    }

    /** Delay before retry number {@code retry} (1-based), drawn with full jitter. */
    public Duration delayBeforeRetry(int retry, RandomGenerator random) {
        if (retry < 1) {
            throw new IllegalArgumentException("retry is 1-based");
        }
        long ceilingMillis = cappedExponentialMillis(retry);
        return ceilingMillis == 0 ? Duration.ZERO : Duration.ofMillis(random.nextLong(ceilingMillis + 1));
    }

    long cappedExponentialMillis(int retry) {
        long base = baseDelay.toMillis();
        long max = maxDelay.toMillis();
        int shift = Math.min(retry - 1, 62);
        long exp = base == 0 ? 0 : (base > (Long.MAX_VALUE >> shift) ? Long.MAX_VALUE : base << shift);
        return Math.min(exp, max);
    }
}
