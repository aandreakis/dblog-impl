package io.github.aandreakis.dblog.adapter.postgres.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class PostgresPgoutputDecoderTests {
  private static final Instant TX_TIME = Instant.parse("2026-03-20T00:00:00Z");

  private final PostgresPgoutputDecoder decoder = new PostgresPgoutputDecoder();

  @Test
  void defaultsEmptyNamespacesToPgCatalogForRelationAndTypeMessages() throws Exception {
    PostgresPgoutputDecoder.RelationMessage relation =
        (PostgresPgoutputDecoder.RelationMessage)
            decoder.decode(
                ByteBuffer.wrap(
                    relation(
                        7,
                        "",
                        "pg_class",
                        'd',
                        new RelationColumnSpec("oid", true, 26))));
    PostgresPgoutputDecoder.TypeMessage type =
        (PostgresPgoutputDecoder.TypeMessage)
            decoder.decode(ByteBuffer.wrap(type(25, "", "text")));

    assertThat(relation.namespace()).isEqualTo("pg_catalog");
    assertThat(relation.relationName()).isEqualTo("pg_class");
    assertThat(relation.columns())
        .containsExactly(new PostgresPgoutputDecoder.RelationColumn(true, "oid", 26, -1));
    assertThat(type.namespace()).isEqualTo("pg_catalog");
    assertThat(type.typeName()).isEqualTo("text");
  }

  @Test
  void decodesOriginLogicalTruncateAndStreamingMessages() throws Exception {
    PostgresPgoutputDecoder.OriginMessage origin =
        (PostgresPgoutputDecoder.OriginMessage)
            decoder.decode(ByteBuffer.wrap(origin("0/16DA010", "remote-node")));
    PostgresPgoutputDecoder.LogicalMessage logical =
        (PostgresPgoutputDecoder.LogicalMessage)
            decoder.decode(
                ByteBuffer.wrap(logicalMessage(1, "0/16DA020", "prefix", new byte[] {1, 2, 3})));
    PostgresPgoutputDecoder.TruncateMessage truncate =
        (PostgresPgoutputDecoder.TruncateMessage)
            decoder.decode(ByteBuffer.wrap(truncate(3, 7, 8)));
    PostgresPgoutputDecoder.StreamStartMessage streamStart =
        (PostgresPgoutputDecoder.StreamStartMessage)
            decoder.decode(ByteBuffer.wrap(streamStart(42, true)));
    PostgresPgoutputDecoder.StreamCommitMessage streamCommit =
        (PostgresPgoutputDecoder.StreamCommitMessage)
            decoder.decode(ByteBuffer.wrap(streamCommit(42, 0, "0/16DA030", "0/16DA038", TX_TIME)));
    PostgresPgoutputDecoder.StreamAbortMessage streamAbort =
        (PostgresPgoutputDecoder.StreamAbortMessage)
            decoder.decode(ByteBuffer.wrap(streamAbort(42, 7)));

    assertThat(origin.originCommitLsn()).isEqualTo(PostgresLsn.parse("0/16DA010"));
    assertThat(origin.originName()).isEqualTo("remote-node");
    assertThat(logical.flags()).isEqualTo(1);
    assertThat(logical.messageLsn()).isEqualTo(PostgresLsn.parse("0/16DA020"));
    assertThat(logical.prefix()).isEqualTo("prefix");
    assertThat(logical.content()).containsExactly((byte) 1, (byte) 2, (byte) 3);
    assertThat(truncate.options()).isEqualTo(3);
    assertThat(truncate.relationIds()).containsExactly(7, 8);
    assertThat(streamStart.transactionId()).isEqualTo(42);
    assertThat(streamStart.firstSegment()).isTrue();
    assertThat(decoder.decode(ByteBuffer.wrap(streamStop())))
        .isSameAs(PostgresPgoutputDecoder.StreamStopMessage.INSTANCE);
    assertThat(streamCommit.transactionId()).isEqualTo(42);
    assertThat(streamCommit.commitLsn()).isEqualTo(PostgresLsn.parse("0/16DA030"));
    assertThat(streamCommit.endLsn()).isEqualTo(PostgresLsn.parse("0/16DA038"));
    assertThat(streamCommit.commitTimestamp()).isEqualTo(TX_TIME);
    assertThat(streamAbort.transactionId()).isEqualTo(42);
    assertThat(streamAbort.subtransactionId()).isEqualTo(7);
  }

  @Test
  void decodesPreparedTransactionMessages() throws Exception {
    PostgresPgoutputDecoder.BeginPrepareMessage beginPrepare =
        (PostgresPgoutputDecoder.BeginPrepareMessage)
            decoder.decode(
                ByteBuffer.wrap(beginPrepare("0/16DA010", "0/16DA018", TX_TIME, 77, "gid-77")));
    PostgresPgoutputDecoder.PrepareMessage prepare =
        (PostgresPgoutputDecoder.PrepareMessage)
            decoder.decode(
                ByteBuffer.wrap(prepare("0/16DA020", "0/16DA028", TX_TIME, 77, "gid-77", 0)));
    PostgresPgoutputDecoder.CommitPreparedMessage commitPrepared =
        (PostgresPgoutputDecoder.CommitPreparedMessage)
            decoder.decode(
                ByteBuffer.wrap(commitPrepared("0/16DA030", "0/16DA038", TX_TIME, 77, "gid-77", 0)));
    PostgresPgoutputDecoder.RollbackPreparedMessage rollbackPrepared =
        (PostgresPgoutputDecoder.RollbackPreparedMessage)
            decoder.decode(
                ByteBuffer.wrap(rollbackPrepared("0/16DA040", "0/16DA048", TX_TIME, 77, "gid-77", 0)));

    assertThat(beginPrepare.prepareLsn()).isEqualTo(PostgresLsn.parse("0/16DA010"));
    assertThat(beginPrepare.endLsn()).isEqualTo(PostgresLsn.parse("0/16DA018"));
    assertThat(beginPrepare.prepareTimestamp()).isEqualTo(TX_TIME);
    assertThat(beginPrepare.transactionId()).isEqualTo(77);
    assertThat(beginPrepare.gid()).isEqualTo("gid-77");

    assertThat(prepare.prepareLsn()).isEqualTo(PostgresLsn.parse("0/16DA020"));
    assertThat(prepare.endLsn()).isEqualTo(PostgresLsn.parse("0/16DA028"));
    assertThat(prepare.prepareTimestamp()).isEqualTo(TX_TIME);
    assertThat(prepare.transactionId()).isEqualTo(77);
    assertThat(prepare.gid()).isEqualTo("gid-77");

    assertThat(commitPrepared.commitLsn()).isEqualTo(PostgresLsn.parse("0/16DA030"));
    assertThat(commitPrepared.endLsn()).isEqualTo(PostgresLsn.parse("0/16DA038"));
    assertThat(commitPrepared.commitTimestamp()).isEqualTo(TX_TIME);
    assertThat(commitPrepared.transactionId()).isEqualTo(77);
    assertThat(commitPrepared.gid()).isEqualTo("gid-77");

    assertThat(rollbackPrepared.rollbackLsn()).isEqualTo(PostgresLsn.parse("0/16DA040"));
    assertThat(rollbackPrepared.endLsn()).isEqualTo(PostgresLsn.parse("0/16DA048"));
    assertThat(rollbackPrepared.rollbackTimestamp()).isEqualTo(TX_TIME);
    assertThat(rollbackPrepared.transactionId()).isEqualTo(77);
    assertThat(rollbackPrepared.gid()).isEqualTo("gid-77");
  }

  @Test
  void rejectsTrailingBytesAfterDecode() throws Exception {
    byte[] valid = origin("0/16DA010", "remote-node");
    byte[] withTrailingByte = java.util.Arrays.copyOf(valid, valid.length + 1);
    withTrailingByte[withTrailingByte.length - 1] = 0x01;

    assertThatThrownBy(() -> decoder.decode(ByteBuffer.wrap(withTrailingByte)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("trailing bytes after decode");
  }

  @Test
  void rejectsUnsupportedBinaryAndUnknownTupleValueMarkers() throws Exception {
    assertThatThrownBy(() -> decoder.decode(ByteBuffer.wrap(insertWithValueMarker('b'))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("binary tuple values are not supported");

    assertThatThrownBy(() -> decoder.decode(ByteBuffer.wrap(insertWithValueMarker('x'))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("unknown value marker");
  }

  private static byte[] relation(
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

  private static byte[] type(int oid, String namespace, String typeName) throws Exception {
    return message(
        'Y',
        out -> {
          out.writeInt(oid);
          writeCString(out, namespace);
          writeCString(out, typeName);
        });
  }

  private static byte[] origin(String lsn, String originName) throws Exception {
    return message(
        'O',
        out -> {
          writeLsn(out, lsn);
          writeCString(out, originName);
        });
  }

  private static byte[] logicalMessage(int flags, String lsn, String prefix, byte[] content)
      throws Exception {
    return message(
        'M',
        out -> {
          out.writeByte(flags);
          writeLsn(out, lsn);
          writeCString(out, prefix);
          out.writeInt(content.length);
          out.write(content);
        });
  }

  private static byte[] truncate(int options, int... relationIds) throws Exception {
    return message(
        'T',
        out -> {
          out.writeInt(relationIds.length);
          out.writeByte(options);
          for (int relationId : relationIds) {
            out.writeInt(relationId);
          }
        });
  }

  private static byte[] streamStart(int transactionId, boolean firstSegment) throws Exception {
    return message(
        'S',
        out -> {
          out.writeInt(transactionId);
          out.writeByte(firstSegment ? 1 : 0);
        });
  }

  private static byte[] streamStop() throws Exception {
    return message('E', out -> {});
  }

  private static byte[] streamCommit(
      int transactionId, int flags, String commitLsn, String endLsn, Instant timestamp)
      throws Exception {
    return message(
        'c',
        out -> {
          out.writeInt(transactionId);
          out.writeByte(flags);
          writeLsn(out, commitLsn);
          writeLsn(out, endLsn);
          writeTimestamp(out, timestamp);
        });
  }

  private static byte[] streamAbort(int transactionId, int subtransactionId) throws Exception {
    return message(
        'A',
        out -> {
          out.writeInt(transactionId);
          out.writeInt(subtransactionId);
        });
  }

  private static byte[] beginPrepare(
      String prepareLsn, String endLsn, Instant timestamp, int transactionId, String gid)
      throws Exception {
    return message(
        'b',
        out -> {
          writeLsn(out, prepareLsn);
          writeLsn(out, endLsn);
          writeTimestamp(out, timestamp);
          out.writeInt(transactionId);
          writeCString(out, gid);
        });
  }

  private static byte[] prepare(
      String prepareLsn, String endLsn, Instant timestamp, int transactionId, String gid, int flags)
      throws Exception {
    return message(
        'P',
        out -> {
          out.writeByte(flags);
          writeLsn(out, prepareLsn);
          writeLsn(out, endLsn);
          writeTimestamp(out, timestamp);
          out.writeInt(transactionId);
          writeCString(out, gid);
        });
  }

  private static byte[] commitPrepared(
      String commitLsn, String endLsn, Instant timestamp, int transactionId, String gid, int flags)
      throws Exception {
    return message(
        'K',
        out -> {
          out.writeByte(flags);
          writeLsn(out, commitLsn);
          writeLsn(out, endLsn);
          writeTimestamp(out, timestamp);
          out.writeInt(transactionId);
          writeCString(out, gid);
        });
  }

  private static byte[] rollbackPrepared(
      String rollbackLsn, String endLsn, Instant timestamp, int transactionId, String gid, int flags)
      throws Exception {
    return message(
        'r',
        out -> {
          out.writeByte(flags);
          writeLsn(out, rollbackLsn);
          writeLsn(out, endLsn);
          writeTimestamp(out, timestamp);
          out.writeInt(transactionId);
          writeCString(out, gid);
        });
  }

  private static byte[] insertWithValueMarker(char marker) throws Exception {
    return message(
        'I',
        out -> {
          out.writeInt(7);
          out.writeByte('N');
          out.writeShort(1);
          out.writeByte((byte) marker);
          if (marker == 'b' || marker == 't') {
            out.writeInt(1);
            out.writeByte('x');
          }
        });
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

  private record RelationColumnSpec(String name, boolean key, int typeOid) {}
}
