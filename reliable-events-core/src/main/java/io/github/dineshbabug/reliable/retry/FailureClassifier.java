package io.github.dineshbabug.reliable.retry;

import java.util.Set;

/** Decides whether a failure is worth retrying. Poison messages should go straight to the dead-letter topic. */
@FunctionalInterface
public interface FailureClassifier {

    boolean isRetryable(Throwable failure);

    /** Retry everything except the given exception types (and their subclasses). */
    static FailureClassifier retryAllExcept(Set<Class<? extends Throwable>> nonRetryable) {
        Set<Class<? extends Throwable>> types = Set.copyOf(nonRetryable);
        return failure -> types.stream().noneMatch(t -> t.isInstance(failure));
    }
}
