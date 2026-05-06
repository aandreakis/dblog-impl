package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.model.TableId;

/**
 * How a dialect interprets a {@link TableId}'s three slots and how many dot-separated parts a
 * configured captured-table entry must have.
 *
 * <p>{@link #requiredParts()} drives the dialect-neutral string validation in {@link
 * RelationalSourceConfigValidator}. The enum constant itself documents which {@code TableId} slot
 * carries which identifier part for each dialect family, so generic consumers (future multi-dialect
 * shared code) can switch on the policy instead of hard-coding a convention.
 */
public enum TableNamingPolicy {
  /**
   * PostgreSQL-style: configured captured-table entries are {@code schema.table}; in {@code
   * TableId}, {@code databaseName} is the configured source database, {@code schemaName} is the
   * PostgreSQL schema, {@code tableName} is the table.
   */
  TWO_PART_SCHEMA_TABLE(2),

  /**
   * MySQL-style: configured captured-table entries are {@code database.table}; in {@code TableId},
   * {@code databaseName} is the logical source id, {@code schemaName} is the actual MySQL
   * database, {@code tableName} is the table.
   */
  TWO_PART_DATABASE_TABLE(2),

  /**
   * SQL Server-style: configured captured-table entries are {@code catalog.schema.table}; in {@code
   * TableId}, the three slots map directly.
   */
  THREE_PART_CATALOG_SCHEMA_TABLE(3);

  private final int requiredParts;

  TableNamingPolicy(int requiredParts) {
    this.requiredParts = requiredParts;
  }

  public int requiredParts() {
    return requiredParts;
  }
}
