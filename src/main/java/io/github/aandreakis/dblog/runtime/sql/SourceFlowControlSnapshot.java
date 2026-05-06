package io.github.aandreakis.dblog.runtime.sql;

/** Operator-facing snapshot of source-log flow control and bounded buffering state. */
public record SourceFlowControlSnapshot(
    Mode mode,
    int queueCapacity,
    int queueDepth,
    int maxQueueDepth,
    boolean queueFull,
    boolean sourceFetchPaused,
    String pausedSince,
    double pauseAgeSeconds,
    double totalPausedSeconds,
    long totalEnqueued,
    long totalDequeued,
    long totalPauseCount,
    double enqueueRatePerSecond,
    double dequeueRatePerSecond) {
  public SourceFlowControlSnapshot {
    mode = mode == null ? Mode.UNAVAILABLE : mode;
    queueCapacity = Math.max(0, queueCapacity);
    queueDepth = Math.max(0, queueDepth);
    maxQueueDepth = Math.max(0, maxQueueDepth);
    totalEnqueued = Math.max(0L, totalEnqueued);
    totalDequeued = Math.max(0L, totalDequeued);
    totalPauseCount = Math.max(0L, totalPauseCount);
    pauseAgeSeconds = Math.max(0.0d, pauseAgeSeconds);
    totalPausedSeconds = Math.max(0.0d, totalPausedSeconds);
    enqueueRatePerSecond = Math.max(0.0d, enqueueRatePerSecond);
    dequeueRatePerSecond = Math.max(0.0d, dequeueRatePerSecond);
  }

  public static SourceFlowControlSnapshot unavailable() {
    return new SourceFlowControlSnapshot(
        Mode.UNAVAILABLE, 0, 0, 0, false, false, null, 0.0d, 0.0d, 0L, 0L, 0L, 0.0d, 0.0d);
  }

  public static SourceFlowControlSnapshot directPoll() {
    return new SourceFlowControlSnapshot(
        Mode.DIRECT_POLL, 0, 0, 0, false, false, null, 0.0d, 0.0d, 0L, 0L, 0L, 0.0d, 0.0d);
  }

  public enum Mode {
    UNAVAILABLE,
    DIRECT_POLL,
    BOUNDED_QUEUE
  }
}
