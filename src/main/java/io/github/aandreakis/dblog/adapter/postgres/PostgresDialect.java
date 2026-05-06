package io.github.aandreakis.dblog.adapter.postgres;

import io.github.aandreakis.dblog.adapter.api.RawColumnMetadata;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfigValidator;
import io.github.aandreakis.dblog.adapter.api.SourceDialect;
import io.github.aandreakis.dblog.adapter.api.TableNamingPolicy;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import java.util.Locale;
import java.util.Objects;

/** PostgreSQL implementation of the {@link SourceDialect} port. */
public final class PostgresDialect implements SourceDialect {
  public static final PostgresDialect INSTANCE = new PostgresDialect();

  public static final String JDBC_PREFIX = "jdbc:postgresql://";
  public static final String DISPLAY_NAME = "PostgreSQL";

  private PostgresDialect() {}

  @Override
  public String key() {
    return "postgres";
  }

  @Override
  public String displayName() {
    return DISPLAY_NAME;
  }

  @Override
  public String jdbcUrlPrefix() {
    return JDBC_PREFIX;
  }

  @Override
  public TableNamingPolicy tableNamingPolicy() {
    return TableNamingPolicy.TWO_PART_SCHEMA_TABLE;
  }

  @Override
  public void validateNativeConfig(RelationalSourceConfig config) {
    RelationalSourceConfigValidator.requireJdbcPrefix(config, JDBC_PREFIX, DISPLAY_NAME);
    RelationalSourceConfigValidator.requireTablePartCount(
        config, requiredTableNameParts(), DISPLAY_NAME);
    requirePostgresDatabaseConsistency(config);
  }

  @Override
  public NeutralColumnType neutralType(RawColumnMetadata column) {
    return neutralType(column.dataType(), column.typeKind());
  }

  public NeutralColumnType neutralType(String typeName, String typeKind) {
    String normalizedTypeName = normalize(typeName);
    if ("e".equals(typeKind)) {
      return NeutralColumnType.ENUM_STRING;
    }
    return switch (normalizedTypeName) {
      case "bool" -> NeutralColumnType.BOOLEAN;
      case "int2", "int4", "int8" -> NeutralColumnType.INTEGER;
      case "float4", "float8" -> NeutralColumnType.FLOAT;
      case "numeric" -> NeutralColumnType.DECIMAL;
      case "varchar", "bpchar", "text" -> NeutralColumnType.STRING;
      case "bytea" -> NeutralColumnType.BINARY;
      case "uuid" -> NeutralColumnType.UUID;
      case "xml" -> NeutralColumnType.XML;
      case "date" -> NeutralColumnType.DATE;
      case "time", "timetz" -> NeutralColumnType.TIME;
      case "timestamp", "timestamptz" -> NeutralColumnType.TIMESTAMP;
      case "json", "jsonb" -> NeutralColumnType.JSON;
      default -> NeutralColumnType.UNSUPPORTED;
    };
  }

  private static String normalize(String value) {
    Objects.requireNonNull(value, "value");
    String normalized = value.trim().toLowerCase(Locale.ROOT);
    if (normalized.isBlank()) {
      throw new IllegalArgumentException("typeName must not be blank");
    }
    return normalized;
  }

  private static void requirePostgresDatabaseConsistency(RelationalSourceConfig config) {
    String jdbcDatabase =
        RelationalSourceConfigValidator.databaseNameFromJdbcUrl(
            config.jdbcUrl(), JDBC_PREFIX, DISPLAY_NAME);
    if (config.databaseName() != null && !jdbcDatabase.equals(config.databaseName())) {
      throw new IllegalArgumentException(
          "Configured PostgreSQL database does not match JDBC URL database. jdbcUrl="
              + jdbcDatabase
              + " configured="
              + config.databaseName());
    }
  }
}
