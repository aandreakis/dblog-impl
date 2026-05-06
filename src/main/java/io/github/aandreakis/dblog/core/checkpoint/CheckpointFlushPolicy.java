package io.github.aandreakis.dblog.core.checkpoint;

import java.time.Duration;
import java.util.Objects;

/**
 * Flush thresholds for ordinary streaming checkpoint acknowledgement.
 *
 * <p>The runtime keeps the latest durably persisted source transaction as a pending checkpoint
 * candidate and acknowledges it only when either the buffered event-count threshold or the
 * buffered-time threshold is reached.
 *
 * <p>This policy applies to ordinary streaming flow only. Dump-window and targeted-repair
 * completion may still advance the source checkpoint immediately once their own local progress or
 * request state is durable.
 */
public record CheckpointFlushPolicy(int maxBufferedEvents, Duration maxBufferedTime) {
  public static final int DEFAULT_MAX_BUFFERED_EVENTS = 100;
  public static final Duration DEFAULT_MAX_BUFFERED_TIME = Duration.ofSeconds(5);

  public CheckpointFlushPolicy {
    if (maxBufferedEvents <= 0) {
      throw new IllegalArgumentException("maxBufferedEvents must be > 0");
    }
    maxBufferedTime = Objects.requireNonNull(maxBufferedTime, "maxBufferedTime");
    if (maxBufferedTime.isZero() || maxBufferedTime.isNegative()) {
      throw new IllegalArgumentException("maxBufferedTime must be > 0");
    }
  }

  public static CheckpointFlushPolicy defaults() {
    return new CheckpointFlushPolicy(DEFAULT_MAX_BUFFERED_EVENTS, DEFAULT_MAX_BUFFERED_TIME);
  }
}
