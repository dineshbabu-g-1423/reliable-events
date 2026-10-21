package io.github.dineshbabug.reliable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dineshbabug.reliable.backpressure.InFlightGate;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class InFlightGateTest {

    private final List<String> events = new ArrayList<>();
    private final InFlightGate gate = new InFlightGate(10, 0.7, new InFlightGate.Listener() {
        @Override public void onPause(int inFlight) { events.add("pause@" + inFlight); }
        @Override public void onResume(int inFlight) { events.add("resume@" + inFlight); }
    });

    @Test
    void pausesAtTheCeilingAndResumesOnlyBelowTheLowWatermark() {
        for (int i = 0; i < 10; i++) gate.acquire();
        assertThat(gate.isPaused()).isTrue();

        for (int i = 0; i < 2; i++) gate.release();   // 8 in flight: still above 7
        assertThat(gate.isPaused()).isTrue();

        gate.release();                                // 7 in flight: resume
        assertThat(gate.isPaused()).isFalse();
        assertThat(events).containsExactly("pause@10", "resume@7");
    }

    @Test
    void doesNotFlapNearTheLimit() {
        for (int i = 0; i < 10; i++) gate.acquire();
        gate.release();
        gate.acquire();
        gate.release();
        assertThat(events).containsExactly("pause@10");
    }

    @Test
    void unmatchedReleaseIsRejected() {
        assertThatThrownBy(gate::release).isInstanceOf(IllegalStateException.class);
        assertThat(gate.inFlight()).isZero();
    }
}
