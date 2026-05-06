package io.github.aandreakis.dblog.adapter.api;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Dialect-agnostic time-based rate limiter for DBLog heartbeat writes.
 *
 * <p>The adapter's heartbeat helper reads the last persisted heartbeat timestamp and calls
 * {@link #shouldSkipWrite(Optional, Instant, Duration)} to decide whether to emit a fresh
 * heartbeat. Pure {@code java.time} arithmetic — no SQL, no dialect, no shared state — so any
 * JDBC-based adapter (MySQL, PostgreSQL, future SQL Server, Oracle) can reuse it.
 *
 * <p>Clock-skew fallback: if the stored timestamp is in the future relative to {@code now},
 * the limiter returns {@code false} (write anyway) rather than silently skipping forever.
 */
public final class HeartbeatRateLimiter {
  private HeartbeatRateLimiter() {}

  /**
   * Returns {@code true} when the caller should skip the heartbeat write — the elapsed time
   * since {@code lastWrite} is non-negative and still strictly less than {@code minimumInterval}.
   * Returns {@code false} when {@code lastWrite} is empty, when the stored timestamp is in the
   * future relative to {@code now} (clock skew), or when at least {@code minimumInterval} has
   * elapsed.
   */
  public static boolean shouldSkipWrite(
      Optional<Instant> lastWrite, Instant now, Duration minimumInterval) {
    Objects.requireNonNull(lastWrite, "lastWrite");
    Objects.requireNonNull(now, "now");
    Objects.requireNonNull(minimumInterval, "minimumInterval");
    if (lastWrite.isEmpty()) {
      return false;
    }
    Duration sinceLast = Duration.between(lastWrite.orElseThrow(), now);
    return !sinceLast.isNegative() && sinceLast.compareTo(minimumInterval) < 0;
  }
}
