package io.github.aandreakis.dblog.sink.jdbc;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/** Startup validation for the JDBC target-apply contract. */
public final class JdbcApplyTargetPreflight {
  private static final JdbcApplyTargetSchemaInspector TARGET_SCHEMA_INSPECTOR =
      new JdbcApplyTargetSchemaInspector();
  private static final TargetConnectionSourceFactory TARGET_CONNECTION_SOURCE_FACTORY =
      (dialect, jdbcUrl, username, password, connectionTimeout) ->
          new HikariTargetConnectionPool(
              dialect.driverClassName(),
              jdbcUrl,
              username,
              password,
              dialect.name().toLowerCase(Locale.ROOT) + "-target-preflight",
              1,
              connectionTimeout);

  @FunctionalInterface
  interface TargetSchemaLookup {
    Optional<JdbcApplyTargetSchemaInspector.TargetTableMetadata> inspect(
        Connection connection, JdbcApplyTargetDialect dialect, TableId tableId) throws SQLException;
  }

  @FunctionalInterface
  interface TargetConnectionSourceFactory {
    JdbcApplyChangeEventSink.SqlConnectionSource open(
        JdbcApplyTargetDialect dialect,
        String jdbcUrl,
        String username,
        String password,
        Duration connectionTimeout);
  }

  private JdbcApplyTargetPreflight() {}

  public static void validate(
      JdbcApplyTargetDialect dialect,
      String jdbcUrl,
      String username,
      String password,
      Duration connectionTimeout,
      List<TableSchema> capturedSchemas) {
    validate(
        dialect,
        jdbcUrl,
        username,
        password,
        connectionTimeout,
        capturedSchemas,
        TargetTableResolver.identity());
  }

  public static void validate(
      JdbcApplyTargetDialect dialect,
      String jdbcUrl,
      String username,
      String password,
      Duration connectionTimeout,
      List<TableSchema> capturedSchemas,
      TargetTableResolver targetTableResolver) {
    validate(
        dialect,
        jdbcUrl,
        username,
        password,
        connectionTimeout,
        capturedSchemas,
        targetTableResolver,
        TARGET_SCHEMA_INSPECTOR::inspect,
        TARGET_CONNECTION_SOURCE_FACTORY);
  }

  static void validate(
      JdbcApplyTargetDialect dialect,
      String jdbcUrl,
      String username,
      String password,
      Duration connectionTimeout,
      List<TableSchema> capturedSchemas,
      TargetTableResolver targetTableResolver,
      TargetSchemaLookup targetSchemaLookup,
      TargetConnectionSourceFactory targetConnectionSourceFactory) {
    Objects.requireNonNull(dialect, "dialect");
    Objects.requireNonNull(capturedSchemas, "capturedSchemas");
    Objects.requireNonNull(targetTableResolver, "targetTableResolver");
    Objects.requireNonNull(targetSchemaLookup, "targetSchemaLookup");
    Objects.requireNonNull(targetConnectionSourceFactory, "targetConnectionSourceFactory");
    if (capturedSchemas.isEmpty()) {
      return;
    }
    String requiredJdbcUrl = requireNonBlank(jdbcUrl, "jdbcUrl");
    requirePositive(connectionTimeout, "connectionTimeout");

    try (JdbcApplyChangeEventSink.SqlConnectionSource connectionSource =
            targetConnectionSourceFactory.open(
                dialect, requiredJdbcUrl, username, password, connectionTimeout);
        Connection connection = connectionSource.open()) {
      validateTargetTables(
          connection,
          dialect,
          capturedSchemas,
          targetTableResolver,
          targetSchemaLookup);
    } catch (TargetApplyOperationException | TargetApplyContractException ex) {
      throw ex;
    } catch (RuntimeException ex) {
      throw ex;
    } catch (SQLException ex) {
      throw TargetApplyFailures.preflightFailed(dialect, jdbcUrl, ex);
    } catch (Exception ex) {
      throw new IllegalStateException(
          "Failed to close target-preflight resources for " + requiredJdbcUrl, ex);
    }
  }

  static void validateTargetTables(
      Connection connection,
      JdbcApplyTargetDialect dialect,
      List<TableSchema> capturedSchemas,
      TargetTableResolver targetTableResolver,
      TargetSchemaLookup targetSchemaLookup)
      throws SQLException {
    Objects.requireNonNull(connection, "connection");
    Objects.requireNonNull(dialect, "dialect");
    Objects.requireNonNull(capturedSchemas, "capturedSchemas");
    Objects.requireNonNull(targetTableResolver, "targetTableResolver");
    Objects.requireNonNull(targetSchemaLookup, "targetSchemaLookup");
    for (TableSchema schema : capturedSchemas) {
      validateTargetTable(connection, dialect, schema, targetTableResolver, targetSchemaLookup);
    }
  }

  private static void validateTargetTable(
      Connection connection,
      JdbcApplyTargetDialect dialect,
      TableSchema schema,
      TargetTableResolver targetTableResolver,
      TargetSchemaLookup targetSchemaLookup)
      throws SQLException {
    TableId targetTableId = targetTableResolver.resolve(schema.tableId());
    JdbcApplyTargetSchemaInspector.TargetTableMetadata targetTable =
        targetSchemaLookup
            .inspect(connection, dialect, targetTableId)
            .orElseThrow(() -> TargetApplyFailures.missingTargetTable(schema.tableId(), targetTableId));

    List<String> targetPrimaryKeyColumns = new ArrayList<>(targetTable.primaryKeyColumns());
    if (!targetTable.matchesPrimaryKeyColumns(schema.primaryKeyColumns())) {
      throw TargetApplyFailures.primaryKeyMismatch(
          schema.tableId(), targetTableId, schema.primaryKeyColumns(), targetPrimaryKeyColumns);
    }

    for (ColumnDefinition column : schema.selectedColumns()) {
      JdbcApplyTargetSchemaInspector.TargetColumnMetadata targetColumn =
          targetTable
              .findColumn(column.name())
              .orElseThrow(
                  () -> TargetApplyFailures.missingTargetColumn(schema.tableId(), targetTableId, column.name()));
      if (!isCompatible(column.neutralType(), targetColumn.neutralType())) {
        throw TargetApplyFailures.incompatibleTargetColumnType(
            schema.tableId(), targetTableId, column, targetColumn);
      }
    }
  }

  private static boolean isCompatible(NeutralColumnType sourceType, NeutralColumnType targetType) {
    return TargetValueCoercions.isCompatible(sourceType, targetType);
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static Duration requirePositive(Duration value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(name + " must be > 0");
    }
    return value;
  }
}
