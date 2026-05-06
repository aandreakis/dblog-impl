package io.github.aandreakis.dblog.adapter.mysql.internal;

import io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition;
import java.time.Duration;
import java.util.Objects;

/** Startup request for the MySQL binlog stream wrapper. */
public record MySqlBinlogStreamRequest(
    String hostname,
    int port,
    String databaseName,
    String username,
    String password,
    long serverId,
    MySqlSourcePosition startPosition,
    boolean useGtidResume,
    Duration connectTimeout,
    Duration heartbeatInterval,
    Duration keepAliveInterval,
    Duration netWriteTimeout,
    int sourceEventQueueCapacity) {
  public MySqlBinlogStreamRequest {
    hostname = requireNonBlank(hostname, "hostname");
    if (port <= 0 || port > 65535) {
      throw new IllegalArgumentException("port must be in the range 1..65535");
    }
    databaseName = requireNonBlank(databaseName, "databaseName");
    username = requireNonBlank(username, "username");
    Objects.requireNonNull(password, "password");
    if (serverId <= 0) {
      throw new IllegalArgumentException("serverId must be > 0");
    }
    if (useGtidResume && (startPosition == null || startPosition.gtidSet() == null)) {
      throw new IllegalArgumentException(
          "useGtidResume requires a startPosition carrying MySQL GTID state");
    }
    if (connectTimeout != null && (connectTimeout.isZero() || connectTimeout.isNegative())) {
      throw new IllegalArgumentException("connectTimeout must be > 0 when present");
    }
    if (heartbeatInterval != null && (heartbeatInterval.isZero() || heartbeatInterval.isNegative())) {
      throw new IllegalArgumentException("heartbeatInterval must be > 0 when present");
    }
    if (keepAliveInterval != null && (keepAliveInterval.isZero() || keepAliveInterval.isNegative())) {
      throw new IllegalArgumentException("keepAliveInterval must be > 0 when present");
    }
    if (netWriteTimeout != null && (netWriteTimeout.isZero() || netWriteTimeout.isNegative())) {
      throw new IllegalArgumentException("netWriteTimeout must be > 0 when present");
    }
    if (heartbeatInterval != null
        && keepAliveInterval != null
        && keepAliveInterval.compareTo(heartbeatInterval) <= 0) {
      throw new IllegalArgumentException(
          "keepAliveInterval must be greater than heartbeatInterval when both are present");
    }
    if (sourceEventQueueCapacity <= 0) {
      throw new IllegalArgumentException("sourceEventQueueCapacity must be > 0");
    }
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
