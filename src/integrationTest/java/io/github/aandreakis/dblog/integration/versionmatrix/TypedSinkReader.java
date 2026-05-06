package io.github.aandreakis.dblog.integration.versionmatrix;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.sink.jdbc.MirrorTableNaming;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Test-side reader for the typed JDBC sink's H2 file. Used after the in-process DBLog runtime has
 * shut down — H2 file mode allows only one connection per file, so the test cannot read the sink
 * concurrently with the runtime that owns it.
 *
 * <p>Mirror table naming is delegated to {@link MirrorTableNaming#deriveMirrorTableName} —
 * the same algorithm the production sink uses to create the mirror, so the test always reads
 * the table the production code wrote.
 */
final class TypedSinkReader {
  private TypedSinkReader() {}

  static String h2JdbcUrl(Path path) {
    return "jdbc:h2:file:"
        + path.toAbsolutePath().normalize().toString().replace('\\', '/')
        + ";DB_CLOSE_ON_EXIT=FALSE";
  }

  /**
   * Returns the row in the typed sink's mirror of {@code table} keyed by a single primary-key
   * column. The caller passes the PK column name explicitly because the sink has no opinion
   * about it — production-side mirror tables preserve whatever the source PK was.
   */
  static Optional<Map<String, Object>> readRowByPrimaryKey(
      Path h2Path, TableId table, String pkColumn, long pkValue) throws Exception {
    String mirror = MirrorTableNaming.deriveMirrorTableName(table);
    String quotedPkColumn = quoteIdentifier(pkColumn);
    try (Connection connection = DriverManager.getConnection(h2JdbcUrl(h2Path));
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT * FROM \"" + mirror + "\" WHERE " + quotedPkColumn + " = ?")) {
      statement.setLong(1, pkValue);
      try (ResultSet rs = statement.executeQuery()) {
        if (!rs.next()) {
          return Optional.empty();
        }
        ResultSetMetaData md = rs.getMetaData();
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= md.getColumnCount(); i++) {
          row.put(md.getColumnLabel(i).toLowerCase(Locale.ROOT), rs.getObject(i));
        }
        return Optional.of(row);
      }
    }
  }

  /** Returns the number of rows currently in a mirror table. */
  static long countRows(Path h2Path, TableId table) throws Exception {
    String mirror = MirrorTableNaming.deriveMirrorTableName(table);
    try (Connection connection = DriverManager.getConnection(h2JdbcUrl(h2Path));
        PreparedStatement statement =
            connection.prepareStatement("SELECT COUNT(*) FROM \"" + mirror + "\"");
        ResultSet rs = statement.executeQuery()) {
      return rs.next() ? rs.getLong(1) : 0L;
    }
  }

  private static String quoteIdentifier(String identifier) {
    if (identifier == null || identifier.isBlank()) {
      throw new IllegalArgumentException("identifier must not be blank");
    }
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  /**
   * Returns a stable per-column descriptor for every {@code MIRROR_*} table in the H2 file. Used
   * to assert the typed sink's schema does not drift across source versions: the same DDL must
   * be reused, never rebuilt.
   */
  static Map<String, String> readSchemaFingerprint(Path h2Path) throws Exception {
    Map<String, String> fingerprint = new TreeMap<>();
    try (Connection connection = DriverManager.getConnection(h2JdbcUrl(h2Path));
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE,"
                    + " CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,"
                    + " IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS"
                    + " WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME LIKE 'MIRROR\\_%' ESCAPE '\\'"
                    + " ORDER BY TABLE_NAME, ORDINAL_POSITION");
        ResultSet rs = statement.executeQuery()) {
      while (rs.next()) {
        String key = rs.getString(1) + "/" + rs.getInt(3) + "/" + rs.getString(2);
        String value =
            rs.getString(4)
                + "|len="
                + rs.getObject(5)
                + "|prec="
                + rs.getObject(6)
                + "|scale="
                + rs.getObject(7)
                + "|null="
                + rs.getString(8);
        fingerprint.put(key, value);
      }
    }
    return fingerprint;
  }
}
