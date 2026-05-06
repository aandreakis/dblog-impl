package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.time.Instant;

final class PostgresPgoutputDecoderTestsHelper {
  private PostgresPgoutputDecoderTestsHelper() {}

  record RelationColumnSpec(String name, boolean key, int typeOid) {}

  static byte[] relation(
      int relationId,
      String schemaName,
      String tableName,
      char replicaIdentity,
      RelationColumnSpec... columns)
      throws Exception {
    return message(
        'R',
        out -> {
          out.writeInt(relationId);
          writeCString(out, schemaName);
          writeCString(out, tableName);
          out.writeByte((byte) replicaIdentity);
          out.writeShort(columns.length);
          for (RelationColumnSpec column : columns) {
            out.writeByte(column.key() ? 1 : 0);
            writeCString(out, column.name());
            out.writeInt(column.typeOid());
            out.writeInt(-1);
          }
        });
  }

  static byte[] begin(String finalLsn, Instant timestamp, int transactionId) throws Exception {
    return message(
        'B',
        out -> {
          writeLsn(out, finalLsn);
          writeTimestamp(out, timestamp);
          out.writeInt(transactionId);
        });
  }

  static byte[] insert(int relationId, String... values) throws Exception {
    return message(
        'I',
        out -> {
          out.writeInt(relationId);
          out.writeByte('N');
          writeTuple(out, values);
        });
  }

  static byte[] update(int relationId, String[] oldValues, String[] newValues) throws Exception {
    return message(
        'U',
        out -> {
          out.writeInt(relationId);
          out.writeByte('O');
          writeTuple(out, oldValues);
          out.writeByte('N');
          writeTuple(out, newValues);
        });
  }

  static byte[] delete(int relationId, String[] oldValues) throws Exception {
    return message(
        'D',
        out -> {
          out.writeInt(relationId);
          out.writeByte('O');
          writeTuple(out, oldValues);
        });
  }

  static byte[] commit(String commitLsn, String endLsn, Instant timestamp, int flags)
      throws Exception {
    return message(
        'C',
        out -> {
          out.writeByte(flags);
          writeLsn(out, commitLsn);
          writeLsn(out, endLsn);
          writeTimestamp(out, timestamp);
        });
  }

  private static void writeTuple(DataOutputStream out, String[] values) throws Exception {
    out.writeShort(values.length);
    for (String value : values) {
      if (value == null) {
        out.writeByte('n');
        continue;
      }
      out.writeByte('t');
      byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }

  private static byte[] message(char type, ThrowingConsumer<DataOutputStream> writer)
      throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      out.writeByte((byte) type);
      writer.accept(out);
    }
    return buffer.toByteArray();
  }

  private static void writeCString(DataOutputStream out, String value) throws Exception {
    out.write(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    out.writeByte(0);
  }

  private static void writeLsn(DataOutputStream out, String value) throws Exception {
    out.writeLong(PostgresLsn.parse(value).asLong());
  }

  private static void writeTimestamp(DataOutputStream out, Instant value) throws Exception {
    long micros =
        java.time.Duration.between(Instant.parse("2000-01-01T00:00:00Z"), value).toNanos()
            / 1_000L;
    out.writeLong(micros);
  }

  @FunctionalInterface
  private interface ThrowingConsumer<T> {
    void accept(T value) throws Exception;
  }
}
