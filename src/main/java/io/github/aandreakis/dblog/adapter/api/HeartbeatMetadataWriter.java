package io.github.aandreakis.dblog.adapter.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

public interface HeartbeatMetadataWriter {
  void ensureHeartbeatTable(Connection connection) throws SQLException;

  boolean writeHeartbeatIfDue(
      Connection connection,
      String runId,
      String sourceStreamId,
      Instant heartbeatTime,
      Duration minimumInterval)
      throws SQLException;
}
