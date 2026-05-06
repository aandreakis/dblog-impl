package io.github.aandreakis.dblog.adapter.mysql.internal;

import io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition;
import java.time.Instant;
import java.util.AbstractList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/** One narrow decoded MySQL binlog message surfaced by the stream wrapper. */
public sealed interface MySqlBinlogMessage
    permits MySqlBinlogMessage.Commit,
        MySqlBinlogMessage.DeleteRows,
        MySqlBinlogMessage.Gtid,
        MySqlBinlogMessage.Query,
        MySqlBinlogMessage.Rotate,
        MySqlBinlogMessage.TableMap,
        MySqlBinlogMessage.UpdateRows,
        MySqlBinlogMessage.WriteRows {
  MySqlSourcePosition position();

  Instant eventTimestamp();

  static WriteRows trustedWriteRows(
      long tableId, List<Object[]> rows, MySqlSourcePosition position, Instant eventTimestamp) {
    return new WriteRows(
        tableId, rows == null ? null : new TrustedRowsList(rows), position, eventTimestamp);
  }

  static DeleteRows trustedDeleteRows(
      long tableId, List<Object[]> rows, MySqlSourcePosition position, Instant eventTimestamp) {
    return new DeleteRows(
        tableId, rows == null ? null : new TrustedRowsList(rows), position, eventTimestamp);
  }

  static UpdateRows trustedUpdateRows(
      long tableId, List<RowChange> rows, MySqlSourcePosition position, Instant eventTimestamp) {
    return new UpdateRows(
        tableId,
        rows == null ? null : new TrustedRowChangesList(rows),
        position,
        eventTimestamp);
  }

  record Gtid(String gtid, MySqlSourcePosition position, Instant eventTimestamp)
      implements MySqlBinlogMessage {
    public Gtid {
      gtid = requireNonBlank(gtid, "gtid");
      position = Objects.requireNonNull(position, "position");
      eventTimestamp = Objects.requireNonNull(eventTimestamp, "eventTimestamp");
    }
  }

  record Query(
      String databaseName, String sql, MySqlSourcePosition position, Instant eventTimestamp)
      implements MySqlBinlogMessage {
    public Query {
      databaseName = normalizeOptional(databaseName);
      sql = requireNonBlank(sql, "sql");
      position = Objects.requireNonNull(position, "position");
      eventTimestamp = Objects.requireNonNull(eventTimestamp, "eventTimestamp");
    }
  }

  record TableMap(
      long tableId,
      String databaseName,
      String tableName,
      byte[] columnTypes,
      int[] columnMetadata,
      BitSet columnNullability,
      List<String> columnNames,
      List<Integer> primaryKeyColumnIndexes,
      BitSet signedness,
      List<String[]> enumValues,
      MySqlSourcePosition position,
      Instant eventTimestamp)
      implements MySqlBinlogMessage {
    public TableMap(
        long tableId,
        String databaseName,
        String tableName,
        MySqlSourcePosition position,
        Instant eventTimestamp) {
      this(
          tableId,
          databaseName,
          tableName,
          null,
          null,
          null,
          List.of(),
          List.of(),
          null,
          List.of(),
          position,
          eventTimestamp);
    }

    public TableMap {
      if (tableId < 0) {
        throw new IllegalArgumentException("tableId must be >= 0");
      }
      databaseName = requireNonBlank(databaseName, "databaseName");
      tableName = requireNonBlank(tableName, "tableName");
      columnTypes = columnTypes == null ? null : columnTypes.clone();
      columnMetadata = columnMetadata == null ? null : columnMetadata.clone();
      columnNullability = columnNullability == null ? null : (BitSet) columnNullability.clone();
      columnNames = columnNames == null ? List.of() : List.copyOf(columnNames);
      primaryKeyColumnIndexes =
          primaryKeyColumnIndexes == null ? List.of() : List.copyOf(primaryKeyColumnIndexes);
      signedness = signedness == null ? null : (BitSet) signedness.clone();
      enumValues = enumValues == null ? List.of() : List.copyOf(enumValues);
      position = Objects.requireNonNull(position, "position");
      eventTimestamp = Objects.requireNonNull(eventTimestamp, "eventTimestamp");
    }
  }

  record WriteRows(
      long tableId, List<Object[]> rows, MySqlSourcePosition position, Instant eventTimestamp)
      implements MySqlBinlogMessage {
    public WriteRows {
      if (tableId < 0) {
        throw new IllegalArgumentException("tableId must be >= 0");
      }
      rows =
          rows instanceof TrustedRowsList
              ? Objects.requireNonNull(rows, "rows")
              : List.copyOf(Objects.requireNonNull(rows, "rows"));
      position = Objects.requireNonNull(position, "position");
      eventTimestamp = Objects.requireNonNull(eventTimestamp, "eventTimestamp");
    }
  }

  record UpdateRows(
      long tableId, List<RowChange> rows, MySqlSourcePosition position, Instant eventTimestamp)
      implements MySqlBinlogMessage {
    public UpdateRows {
      if (tableId < 0) {
        throw new IllegalArgumentException("tableId must be >= 0");
      }
      rows =
          rows instanceof TrustedRowChangesList
              ? Objects.requireNonNull(rows, "rows")
              : List.copyOf(Objects.requireNonNull(rows, "rows"));
      position = Objects.requireNonNull(position, "position");
      eventTimestamp = Objects.requireNonNull(eventTimestamp, "eventTimestamp");
    }
  }

  record DeleteRows(
      long tableId, List<Object[]> rows, MySqlSourcePosition position, Instant eventTimestamp)
      implements MySqlBinlogMessage {
    public DeleteRows {
      if (tableId < 0) {
        throw new IllegalArgumentException("tableId must be >= 0");
      }
      rows =
          rows instanceof TrustedRowsList
              ? Objects.requireNonNull(rows, "rows")
              : List.copyOf(Objects.requireNonNull(rows, "rows"));
      position = Objects.requireNonNull(position, "position");
      eventTimestamp = Objects.requireNonNull(eventTimestamp, "eventTimestamp");
    }
  }

  record Commit(String transactionId, MySqlSourcePosition position, Instant eventTimestamp)
      implements MySqlBinlogMessage {
    public Commit {
      transactionId = requireNonBlank(transactionId, "transactionId");
      position = Objects.requireNonNull(position, "position");
      eventTimestamp = Objects.requireNonNull(eventTimestamp, "eventTimestamp");
    }
  }

  record Rotate(
      String nextBinlogFilename,
      long nextBinlogPosition,
      MySqlSourcePosition position,
      Instant eventTimestamp)
      implements MySqlBinlogMessage {
    public Rotate {
      nextBinlogFilename = requireNonBlank(nextBinlogFilename, "nextBinlogFilename");
      if (nextBinlogPosition < 0) {
        throw new IllegalArgumentException("nextBinlogPosition must be >= 0");
      }
      position = Objects.requireNonNull(position, "position");
      eventTimestamp = Objects.requireNonNull(eventTimestamp, "eventTimestamp");
    }
  }

  record RowChange(Object[] beforeValues, Object[] afterValues) {
    public RowChange {
      beforeValues = beforeValues == null ? null : beforeValues.clone();
      afterValues = afterValues == null ? null : afterValues.clone();
      if (beforeValues == null || afterValues == null) {
        throw new IllegalArgumentException(
            "MySQL update row changes require both beforeValues and afterValues");
      }
    }
  }

  final class TrustedRowsList extends AbstractList<Object[]> {
    private final List<Object[]> delegate;

    private TrustedRowsList(List<Object[]> delegate) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public Object[] get(int index) {
      return delegate.get(index);
    }

    @Override
    public int size() {
      return delegate.size();
    }
  }

  final class TrustedRowChangesList extends AbstractList<RowChange> {
    private final List<RowChange> delegate;

    private TrustedRowChangesList(List<RowChange> delegate) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public RowChange get(int index) {
      return delegate.get(index);
    }

    @Override
    public int size() {
      return delegate.size();
    }
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static String normalizeOptional(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value;
  }
}
