package io.github.dineshbabug.reliable.processing;

import java.time.Duration;

/** Indirection over {@link Thread#sleep} so retry timing can be tested without waiting. */
@FunctionalInterface
public interface Sleeper {
    void sleep(Duration duration) throws InterruptedException;

    Sleeper THREAD = d -> Thread.sleep(d.toMillis());
}
