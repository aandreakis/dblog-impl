package io.github.aandreakis.dblog.sink.jdbc;

import io.github.aandreakis.dblog.core.model.TableId;
import java.util.Locale;
import java.util.Optional;

/**
 * Single source of truth for the typed JDBC sink's mirror-table naming. Used by both
 * {@link JdbcTypedChangeEventSink} (to create and address the mirror table) and the version-matrix
 * test reader (to address it for inspection assertions). Centralising the algorithm prevents the
 * test from silently drifting out of sync with the production sanitisation rule.
 *
 * <p>Mirror table names are deterministic from the source {@link TableId}:
 * {@code MIRROR_<database>_<schema>_<table>}, with non-{@code [A-Za-z0-9_]} characters folded to
 * underscore, runs of underscores collapsed, leading underscore trimmed, and the final identifier
 * upper-cased. Names longer than {@link #MAX_MIRROR_TABLE_NAME_LENGTH} are truncated and
 * suffixed with a stable hash so two long source names cannot collide on the truncated prefix.
 */
public final class MirrorTableNaming {

  /**
   * Maximum length of a generated mirror table identifier. Keeps mirror names well under H2's
   * 256-char identifier limit and Postgres' 63-char limit, so the same fingerprint can be reused
   * if the typed-h2 sink is ever pointed at a non-H2 backend in tests or operator scripts.
   */
  public static final int MAX_MIRROR_TABLE_NAME_LENGTH = 60;

  private MirrorTableNaming() {}

  /** Derives the mirror table name for {@code tableId}. See class javadoc for the algorithm. */
  public static String deriveMirrorTableName(TableId tableId) {
    String raw =
        Optional.ofNullable(tableId.databaseName()).orElse("")
            + "_"
            + Optional.ofNullable(tableId.schemaName()).orElse("")
            + "_"
            + tableId.tableName();
    String sanitized = raw.replaceAll("[^A-Za-z0-9_]", "_").replaceAll("_+", "_");
    if (sanitized.startsWith("_")) {
      sanitized = sanitized.substring(1);
    }
    if (sanitized.length() > MAX_MIRROR_TABLE_NAME_LENGTH) {
      sanitized =
          sanitized.substring(0, MAX_MIRROR_TABLE_NAME_LENGTH - 9)
              + "_"
              + Integer.toUnsignedString(tableId.displayName().hashCode(), 16);
    }
    return ("MIRROR_" + sanitized).toUpperCase(Locale.ROOT);
  }
}
