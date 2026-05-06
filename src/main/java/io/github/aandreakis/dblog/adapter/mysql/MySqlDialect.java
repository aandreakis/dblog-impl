package io.github.aandreakis.dblog.adapter.mysql;

import io.github.aandreakis.dblog.adapter.api.RawColumnMetadata;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfigValidator;
import io.github.aandreakis.dblog.adapter.api.SourceDialect;
import io.github.aandreakis.dblog.adapter.api.TableNamingPolicy;
import io.github.aandreakis.dblog.adapter.api.ValueDecoder;
import io.github.aandreakis.dblog.adapter.mysql.internal.MySqlValueDecoder;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** MySQL implementation of the {@link SourceDialect} port. */
public final class MySqlDialect implements SourceDialect {
  public static final MySqlDialect INSTANCE = new MySqlDialect();

  public static final String JDBC_PREFIX = "jdbc:mysql://";
  public static final String DISPLAY_NAME = "MySQL";

  private MySqlDialect() {}

  @Override
  public String key() {
    return "mysql";
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
    return TableNamingPolicy.TWO_PART_DATABASE_TABLE;
  }

  @Override
  public void validateNativeConfig(RelationalSourceConfig config) {
    RelationalSourceConfigValidator.requireJdbcPrefix(config, JDBC_PREFIX, DISPLAY_NAME);
    RelationalSourceConfigValidator.requireTablePartCount(
        config, requiredTableNameParts(), DISPLAY_NAME);
    requireMySqlDatabaseConsistency(config);
  }

  @Override
  public NeutralColumnType neutralType(RawColumnMetadata column) {
    return neutralType(column.dataType(), column.columnType());
  }

  @Override
  public String canonicalSourceType(RawColumnMetadata column) {
    return canonicalSourceType(column.dataType(), column.columnType());
  }

  @Override
  public ValueDecoder valueDecoder() {
    return MySqlValueDecoder.INSTANCE;
  }

  public NeutralColumnType neutralType(String dataType, String columnType) {
    String normalized = normalize(dataType);
    String normalizedColumnType =
        normalize(columnType == null || columnType.isBlank() ? dataType : columnType);
    return switch (normalized) {
      case "bool", "boolean" -> NeutralColumnType.BOOLEAN;
      case "tinyint" ->
          normalizedColumnType.startsWith("tinyint(1")
              ? NeutralColumnType.BOOLEAN
              : NeutralColumnType.INTEGER;
      case "bit" ->
          normalizedColumnType.startsWith("bit(1")
              ? NeutralColumnType.BOOLEAN
              : NeutralColumnType.BINARY;
      case "smallint", "mediumint", "int", "integer", "bigint", "year" ->
          NeutralColumnType.INTEGER;
      case "float", "double", "real" -> NeutralColumnType.FLOAT;
      case "decimal", "numeric" -> NeutralColumnType.DECIMAL;
      case "char", "varchar", "text", "tinytext", "mediumtext", "longtext" ->
          NeutralColumnType.STRING;
      case "enum", "set" -> NeutralColumnType.ENUM_STRING;
      case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob" ->
          NeutralColumnType.BINARY;
      case "json" -> NeutralColumnType.JSON;
      case "date" -> NeutralColumnType.DATE;
      case "time" -> NeutralColumnType.TIME;
      case "datetime", "timestamp" -> NeutralColumnType.TIMESTAMP;
      case "uuid" -> NeutralColumnType.UUID;
      default -> NeutralColumnType.UNSUPPORTED;
    };
  }

  public String canonicalSourceType(String dataType, String columnType) {
    String normalizedType = normalize(dataType);
    String raw = normalize(columnType == null || columnType.isBlank() ? dataType : columnType);

    return switch (normalizedType) {
      case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year" ->
          canonicalIntegerLikeSourceType(normalizedType, raw);
      default -> raw;
    };
  }

  private static String canonicalIntegerLikeSourceType(String normalizedType, String raw) {
    String baseType = "integer".equals(normalizedType) ? "int" : normalizedType;
    StringBuilder canonical = new StringBuilder(baseType);
    if (raw.contains(" unsigned")) {
      canonical.append(" unsigned");
    }
    if (raw.contains(" zerofill")) {
      canonical.append(" zerofill");
    }
    return canonical.toString();
  }

  private static String normalize(String value) {
    Objects.requireNonNull(value, "value");
    return value.trim().toLowerCase(Locale.ROOT);
  }

  private static void requireMySqlDatabaseConsistency(RelationalSourceConfig config) {
    String jdbcDatabase =
        RelationalSourceConfigValidator.databaseNameFromJdbcUrl(
            config.jdbcUrl(), JDBC_PREFIX, DISPLAY_NAME);
    String effectiveDatabase =
        config.databaseName() == null ? jdbcDatabase : config.databaseName();
    if (!jdbcDatabase.equals(effectiveDatabase)) {
      throw new IllegalArgumentException(
          "Configured MySQL database does not match JDBC URL database. jdbcUrl="
              + jdbcDatabase
              + " configured="
              + effectiveDatabase);
    }
    List<String> namespaces =
        config.capturedTables().stream()
            .map(table -> table.split("\\.", 2)[0].trim().toLowerCase(Locale.ROOT))
            .distinct()
            .toList();
    if (namespaces.size() != 1
        || !namespaces.getFirst().equals(effectiveDatabase.toLowerCase(Locale.ROOT))) {
      throw new IllegalArgumentException(
          "MySQL capturedTables must all belong to the configured database " + effectiveDatabase);
    }
  }
}
