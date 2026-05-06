package io.github.aandreakis.dblog.controlplane.service;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Small global seam for emitting operator-facing recent-event activity into the control plane. */
public final class ControlPlaneEventCapture {
  interface Recorder {
    void recordEmitted(String stageLabel, List<ChangeEvent> events);
  }

  private static final Recorder NOOP_RECORDER =
      new Recorder() {
        @Override
        public void recordEmitted(String stageLabel, List<ChangeEvent> events) {
          Objects.requireNonNull(stageLabel, "stageLabel");
          Objects.requireNonNull(events, "events");
        }
      };

  private final AtomicLong nextSubscriptionId = new AtomicLong(0L);
  private final ConcurrentMap<Long, Recorder> recorders = new ConcurrentHashMap<>();

  public ControlPlaneEventCapture() {}

  public Subscription register(Recorder recorder) {
    Objects.requireNonNull(recorder, "recorder");
    long subscriptionId = nextSubscriptionId.incrementAndGet();
    recorders.put(subscriptionId, recorder);
    return new Subscription(subscriptionId);
  }

  public void unregister(Recorder recorder) {
    Objects.requireNonNull(recorder, "recorder");
    recorders.entrySet().removeIf(entry -> entry.getValue() == recorder);
  }

  public void recordEmitted(String stageLabel, List<ChangeEvent> events) {
    if (recorders.isEmpty()) {
      NOOP_RECORDER.recordEmitted(stageLabel, events);
      return;
    }
    for (Recorder recorder : recorders.values()) {
      try {
        recorder.recordEmitted(stageLabel, events);
      } catch (RuntimeException ignored) {
        // Best-effort fan-out only; one recorder must not break the others.
      }
    }
  }

  public final class Subscription implements AutoCloseable {
    private final long subscriptionId;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private Subscription(long subscriptionId) {
      this.subscriptionId = subscriptionId;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        recorders.remove(subscriptionId);
      }
    }
  }
}
