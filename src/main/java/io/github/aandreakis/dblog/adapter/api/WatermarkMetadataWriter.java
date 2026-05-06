package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.model.WatermarkToken;
import java.sql.Connection;
import java.sql.SQLException;

public interface WatermarkMetadataWriter {
  void ensureMetadataTable(Connection connection) throws SQLException;

  void writeWatermark(Connection connection, String runId, WatermarkToken token)
      throws SQLException;
}
