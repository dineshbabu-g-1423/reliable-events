package io.github.dineshbabug.reliable.backpressure;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pauses consumption when in-flight work reaches a ceiling and resumes it only after the
 * backlog drains below a lower watermark.
 *
 * <p>The gap between the two thresholds (hysteresis) stops the consumer flapping between
 * paused and resumed when load sits near the limit.
 */
public final class InFlightGate {

    /** Called on state changes, for example to pause or resume a Kafka listener container. */
    public interface Listener {
        void onPause(int inFlight);

        void onResume(int inFlight);
    }

    private final int highWatermark;
    private final int lowWatermark;
    private final Listener listener;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicBoolean paused = new AtomicBoolean();

    /**
     * @param highWatermark pause when in-flight count reaches this value
     * @param resumeRatio   resume when in-flight falls to {@code highWatermark * resumeRatio} or below, in (0, 1)
     */
    public InFlightGate(int highWatermark, double resumeRatio, Listener listener) {
        if (highWatermark < 1) {
            throw new IllegalArgumentException("highWatermark must be at least 1");
        }
        if (!(resumeRatio > 0 && resumeRatio < 1)) {
            throw new IllegalArgumentException("resumeRatio must be between 0 and 1");
        }
        this.highWatermark = highWatermark;
        this.lowWatermark = (int) Math.floor(highWatermark * resumeRatio);
        this.listener = Objects.requireNonNull(listener, "listener");
    }

    /** Record that a message was taken into processing. */
    public void acquire() {
        int now = inFlight.incrementAndGet();
        if (now >= highWatermark && paused.compareAndSet(false, true)) {
            listener.onPause(now);
        }
    }

    /** Record that a message finished processing, successfully or not. */
    public void release() {
        int now = inFlight.decrementAndGet();
        if (now < 0) {
            inFlight.incrementAndGet();
            throw new IllegalStateException("release without matching acquire");
        }
        if (now <= lowWatermark && paused.compareAndSet(true, false)) {
            listener.onResume(now);
        }
    }

    public int inFlight() { return inFlight.get(); }
    public boolean isPaused() { return paused.get(); }
    public int highWatermark() { return highWatermark; }
    public int lowWatermark() { return lowWatermark; }
}
