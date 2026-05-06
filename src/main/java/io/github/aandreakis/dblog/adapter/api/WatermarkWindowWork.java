package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface WatermarkWindowWork<T> {
  T execute(Connection connection, WatermarkWindow window) throws SQLException;
}
