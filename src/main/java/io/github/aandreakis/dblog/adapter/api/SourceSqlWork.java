package io.github.aandreakis.dblog.adapter.api;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface SourceSqlWork<T> {
  T execute(Connection connection) throws SQLException;
}
