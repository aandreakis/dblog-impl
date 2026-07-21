package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.List;
import java.util.Objects;

/** Capture-time PostgreSQL primary-key constraints that neutral values cannot preserve. */
final class PostgresPrimaryKeyPolicy {
  private PostgresPrimaryKeyPolicy() {}

  static void requireSupportedForCapture(List<TableSchema> schemas) {
    Objects.requireNonNull(schemas, "schemas");
    schemas.forEach(PostgresPrimaryKeyPolicy::requireSupportedForCapture);
  }

  static void requireSupportedForCapture(TableSchema schema) {
    Objects.requireNonNull(schema, "schema");
    List<String> lossyColumns =
        schema.primaryKeyDefinitions().stream()
            .filter(ColumnDefinition::isTimeWithTimeZone)
            .map(ColumnDefinition::name)
            .toList();
    if (!lossyColumns.isEmpty()) {
      throw new IllegalStateException(
          "PostgreSQL TIMETZ primary key columns are unsupported for captured table "
              + schema.tableId().displayName()
              + " because the neutral TIME representation discards their UTC offsets: "
              + String.join(", ", lossyColumns));
    }
  }
}
