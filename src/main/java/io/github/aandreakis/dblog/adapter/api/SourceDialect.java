package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.schema.NeutralColumnType;

/**
 * Per-adapter port that owns the dialect-specific knowledge a source database needs: JDBC URL
 * shape, identifier quoting, captured-table naming, config validation, and the mapping from
 * raw column metadata to {@link NeutralColumnType}.
 *
 * <p>Concrete dialects live inside each adapter module (e.g. {@code adapter.mysql.internal} and
 * {@code adapter.postgres.internal}). The semantic core and the generic runtime never depend on
 * a dialect instance directly; they only depend on this port via {@link SourceAdapter#dialect()}.
 */
public interface SourceDialect {
  /** Stable identifier used by config/registry (e.g. {@code "mysql"}, {@code "postgres"}). */
  String key();

  /** Human-readable display name (e.g. {@code "MySQL"}, {@code "PostgreSQL"}). */
  String displayName();

  /** Required JDBC URL prefix for this dialect (e.g. {@code "jdbc:mysql://"}). */
  String jdbcUrlPrefix();

  /**
   * How this dialect interprets {@link io.github.aandreakis.dblog.core.model.TableId}'s three slots
   * and how many dot-separated parts a configured captured-table entry must have. MySQL returns
   * {@link TableNamingPolicy#TWO_PART_DATABASE_TABLE}; PostgreSQL returns {@link
   * TableNamingPolicy#TWO_PART_SCHEMA_TABLE}; a future SQL Server dialect would return {@link
   * TableNamingPolicy#THREE_PART_CATALOG_SCHEMA_TABLE}.
   */
  TableNamingPolicy tableNamingPolicy();

  /**
   * Number of dot-separated parts that a configured captured-table entry must have. Derived from
   * {@link #tableNamingPolicy()}; dialects should override the policy, not this method.
   */
  default int requiredTableNameParts() {
    return tableNamingPolicy().requiredParts();
  }

  /**
   * Fail-closed validation of all dialect-specific rules on the relational source config:
   * required JDBC URL prefix, captured-table shape, and database-consistency invariants.
   */
  void validateNativeConfig(RelationalSourceConfig config);

  /** Map raw column metadata from this dialect to its {@link NeutralColumnType}. */
  NeutralColumnType neutralType(RawColumnMetadata column);

  /**
   * Canonicalize the raw column source type for stable schema fingerprints. Defaults to returning
   * the unmodified column type; MySQL overrides this to canonicalize integer-like modifiers such
   * as {@code unsigned}/{@code zerofill}.
   */
  default String canonicalSourceType(RawColumnMetadata column) {
    String columnType = column.columnType();
    return columnType == null || columnType.isBlank() ? column.dataType() : columnType;
  }

  /**
   * Returns the dialect-specific {@link ValueDecoder}. Callers that receive raw wire-format values
   * that require dialect-specific decoding (e.g. MySQL binlog binary JSON) delegate here before
   * passing the result to the neutral normalizer. Dialects whose wire format is already
   * normalizer-friendly return {@link ValueDecoder#PASS_THROUGH}.
   */
  default ValueDecoder valueDecoder() {
    return ValueDecoder.PASS_THROUGH;
  }
}
