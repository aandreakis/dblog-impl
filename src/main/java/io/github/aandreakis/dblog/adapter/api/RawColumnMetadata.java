package io.github.aandreakis.dblog.adapter.api;

import java.util.Objects;

/**
 * Adapter-neutral carrier for the dialect-specific raw column metadata that a {@link
 * SourceDialect} needs to classify a column into a {@link
 * io.github.aandreakis.dblog.core.schema.NeutralColumnType}.
 *
 * <p>Fields are optional and their semantics are interpreted per dialect:
 *
 * <ul>
 *   <li><b>MySQL</b> populates {@code dataType} (e.g. {@code tinyint}) and {@code columnType} (e.g.
 *       {@code tinyint(1) unsigned}).
 *   <li><b>PostgreSQL</b> populates {@code dataType} (the {@code pg_type.typname}, e.g. {@code
 *       int8}) and {@code typeKind} (the {@code pg_type.typtype}, e.g. {@code e} for enums).
 * </ul>
 */
public record RawColumnMetadata(String dataType, String columnType, String typeKind) {
  public RawColumnMetadata {
    Objects.requireNonNull(dataType, "dataType");
  }

  public static RawColumnMetadata mysql(String dataType, String columnType) {
    return new RawColumnMetadata(dataType, columnType, null);
  }

  public static RawColumnMetadata postgres(String typeName, String typeKind) {
    return new RawColumnMetadata(typeName, null, typeKind);
  }
}
