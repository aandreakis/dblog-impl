package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.reconcile.MetadataCorruptionException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Dialect-agnostic policy helpers for DBLog metadata tables (watermarks, heartbeats).
 *
 * <p>Each metadata table is a singleton row at {@code id = 1}; every write must update exactly
 * one row. The SQL that gets executed is each adapter's concern — this class only enforces the
 * contract around the {@link PreparedStatement} the caller built. Nothing in here assumes
 * anything about quoting, namespace concepts, upsert syntax, or type mapping — so MySQL,
 * PostgreSQL, and future SQL Server or Oracle adapters can all call through.
 */
public final class SingletonMetadataRowSupport {
  private SingletonMetadataRowSupport() {}

  /**
   * Execute a prepared update on a singleton metadata row, fail-closed if update count is not
   * exactly 1. Throws {@link MetadataCorruptionException} so callers can catch a single typed
   * exception for the singleton contract regardless of which metadata table misbehaved.
   *
   * @param statement the prepared UPDATE/MERGE/ON CONFLICT statement the adapter built
   * @param dialectLabel human-readable dialect name in the error message (e.g. {@code "MySQL"},
   *     {@code "PostgreSQL"})
   * @param metadataKind human-readable metadata-table name in the error message (e.g. {@code
   *     "watermark"}, {@code "heartbeat"})
   */
  public static void executeSingletonUpdate(
      PreparedStatement statement, String dialectLabel, String metadataKind) throws SQLException {
    Objects.requireNonNull(statement, "statement");
    requireNonBlank(dialectLabel, "dialectLabel");
    requireNonBlank(metadataKind, "metadataKind");
    int updatedRows = statement.executeUpdate();
    if (updatedRows != 1) {
      throw new MetadataCorruptionException(
          dialectLabel
              + " "
              + metadataKind
              + " singleton row is missing or duplicated; expected exactly 1 updated row but was "
              + updatedRows);
    }
  }

  /** Fail-closed non-blank string validation shared across adapter metadata helpers. */
  public static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
