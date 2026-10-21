package io.github.dineshbabug.reliable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dineshbabug.reliable.retry.RetryPolicy;
import java.time.Duration;
import java.util.SplittableRandom;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

    private final RetryPolicy policy = new RetryPolicy(5, Duration.ofMillis(100), Duration.ofSeconds(1));

    @RepeatedTest(50)
    void delayStaysWithinTheCappedExponentialCeiling() {
        SplittableRandom random = new SplittableRandom();
        assertThat(policy.delayBeforeRetry(1, random)).isBetween(Duration.ZERO, Duration.ofMillis(100));
        assertThat(policy.delayBeforeRetry(3, random)).isBetween(Duration.ZERO, Duration.ofMillis(400));
        assertThat(policy.delayBeforeRetry(10, random)).isBetween(Duration.ZERO, Duration.ofSeconds(1));
    }

    @Test
    void largeRetryNumbersDoNotOverflow() {
        RetryPolicy huge = new RetryPolicy(100, Duration.ofDays(1), Duration.ofDays(2));
        assertThat(huge.delayBeforeRetry(90, new SplittableRandom(1))).isLessThanOrEqualTo(Duration.ofDays(2));
    }

    @Test
    void attemptBudgetIncludesTheFirstAttempt() {
        assertThat(policy.canRetry(4)).isTrue();
        assertThat(policy.canRetry(5)).isFalse();
        assertThat(RetryPolicy.noRetries().canRetry(1)).isFalse();
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> new RetryPolicy(0, Duration.ZERO, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetryPolicy(3, Duration.ofSeconds(2), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
