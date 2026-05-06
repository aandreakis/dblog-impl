package io.github.aandreakis.dblog.runtime.bootstrap;

import io.github.aandreakis.dblog.config.DbLogProperties;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import java.time.Duration;
import java.util.Objects;
import org.springframework.core.env.Environment;

/** Shared runtime-tuning helpers for the shipped runtime/bootstrap surface. */
public final class RuntimeTuningSupport {
  private RuntimeTuningSupport() {}

  public static int chunkSize(DbLogProperties properties) {
    Objects.requireNonNull(properties, "properties");
    int chunkSize = properties.getChunk().getSize();
    if (chunkSize <= 0) {
      throw new IllegalArgumentException("Configured chunk size must be > 0");
    }
    return chunkSize;
  }

  public static CheckpointFlushPolicy checkpointFlushPolicy(DbLogProperties properties) {
    int maxEvents = Objects.requireNonNull(properties, "properties").getCheckpoint().getMaxEvents();
    Duration maxTime = properties.getCheckpoint().getMaxInterval();
    return new CheckpointFlushPolicy(maxEvents, maxTime);
  }

  public static Duration heartbeatInterval(DbLogProperties properties) {
    Objects.requireNonNull(properties, "properties");
    Duration interval = properties.getHeartbeat().getInterval();
    if (interval == null || interval.isZero() || interval.isNegative()) {
      throw new IllegalArgumentException("Configured heartbeat interval must be > 0");
    }
    return interval;
  }

  public static int chunkSize(Environment environment) {
    Objects.requireNonNull(environment, "environment");
    Integer chunkSize = environment.getProperty("dblog.chunk.size", Integer.class, 100);
    if (chunkSize == null || chunkSize <= 0) {
      throw new IllegalArgumentException("Configured chunk size must be > 0");
    }
    return chunkSize;
  }

  public static CheckpointFlushPolicy checkpointFlushPolicy(Environment environment) {
    Integer maxEvents =
        Objects.requireNonNull(environment, "environment")
            .getProperty(
                "dblog.checkpoint.max-events",
                Integer.class,
                CheckpointFlushPolicy.DEFAULT_MAX_BUFFERED_EVENTS);
    Duration maxTime =
        environment.getProperty(
            "dblog.checkpoint.max-interval",
            Duration.class,
            CheckpointFlushPolicy.DEFAULT_MAX_BUFFERED_TIME);
    return new CheckpointFlushPolicy(maxEvents, maxTime);
  }

  public static Duration heartbeatInterval(Environment environment) {
    Objects.requireNonNull(environment, "environment");
    Duration interval =
        environment.getProperty(
            "dblog.heartbeat.interval",
            Duration.class,
            Duration.ofSeconds(5));
    if (interval == null || interval.isZero() || interval.isNegative()) {
      throw new IllegalArgumentException("Configured heartbeat interval must be > 0");
    }
    return interval;
  }
}
