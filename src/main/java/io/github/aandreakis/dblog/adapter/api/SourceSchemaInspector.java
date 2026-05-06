package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface SourceSchemaInspector {
  List<TableSchema> inspectSchemas(Connection connection) throws SQLException;
}
