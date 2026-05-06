package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Decoder for the narrow PostgreSQL {@code pgoutput} protocol-1 message subset. */
public final class PostgresPgoutputDecoder {
  private static final Instant POSTGRES_EPOCH = Instant.parse("2000-01-01T00:00:00Z");

  public DecodedMessage decode(ByteBuffer rawMessage) {
    try {
      ByteBuffer message = rawMessage.asReadOnlyBuffer();
      if (!message.hasRemaining()) {
        throw new IllegalStateException("pgoutput message buffer was empty");
      }
      DecodedMessage decoded =
          switch (readTag(message)) {
            case 'B' -> new BeginMessage(readLsn(message), readTimestamp(message), message.getInt());
            case 'C' -> decodeCommit(message);
            case 'R' -> decodeRelation(message);
            case 'Y' -> new TypeMessage(message.getInt(), namespaceOrPgCatalog(readString(message)), readString(message));
            case 'O' -> new OriginMessage(readLsn(message), readString(message));
            case 'M' -> decodeLogicalMessage(message);
            case 'I' -> decodeInsert(message);
            case 'U' -> decodeUpdate(message);
            case 'D' -> decodeDelete(message);
            case 'T' -> decodeTruncate(message);
            case 'S' -> decodeStreamStart(message);
            case 'E' -> StreamStopMessage.INSTANCE;
            case 'c' -> decodeStreamCommit(message);
            case 'A' -> decodeStreamAbort(message);
            case 'b' -> decodeBeginPrepare(message);
            case 'P' -> decodePrepare(message);
            case 'K' -> decodeCommitPrepared(message);
            case 'r' -> decodeRollbackPrepared(message);
            default ->
                throw new IllegalStateException(
                    "Unsupported pgoutput message type byte: "
                        + Integer.toUnsignedString(Byte.toUnsignedInt(rawMessage.asReadOnlyBuffer().get(0))));
          };
      if (message.hasRemaining()) {
        throw new IllegalStateException(
            "pgoutput message had trailing bytes after decode: " + decoded.getClass().getSimpleName());
      }
      return decoded;
    } catch (BufferUnderflowException ex) {
      throw new IllegalStateException("pgoutput message ended unexpectedly during decode", ex);
    }
  }

  private static CommitMessage decodeCommit(ByteBuffer message) {
    byte flags = message.get();
    if (flags != 0) {
      throw new IllegalStateException("pgoutput commit flags must currently be zero");
    }
    return new CommitMessage(readLsn(message), readLsn(message), readTimestamp(message));
  }

  private static RelationMessage decodeRelation(ByteBuffer message) {
    int relationId = message.getInt();
    String namespace = namespaceOrPgCatalog(readString(message));
    String relationName = readString(message);
    char replicaIdentity = (char) Byte.toUnsignedInt(message.get());
    int columnCount = Short.toUnsignedInt(message.getShort());
    List<RelationColumn> columns = new ArrayList<>(columnCount);
    for (int index = 0; index < columnCount; index++) {
      int flags = Byte.toUnsignedInt(message.get());
      columns.add(
          new RelationColumn(
              (flags & 0x01) == 0x01,
              readString(message),
              message.getInt(),
              message.getInt()));
    }
    return new RelationMessage(relationId, namespace, relationName, replicaIdentity, List.copyOf(columns));
  }

  private static LogicalMessage decodeLogicalMessage(ByteBuffer message) {
    int flags = Byte.toUnsignedInt(message.get());
    return new LogicalMessage(flags, readLsn(message), readString(message), readBytes(message, message.getInt()));
  }

  private static InsertMessage decodeInsert(ByteBuffer message) {
    int relationId = message.getInt();
    expectTag(message, 'N');
    return new InsertMessage(relationId, readTupleData(message));
  }

  private static UpdateMessage decodeUpdate(ByteBuffer message) {
    int relationId = message.getInt();
    char marker = readTag(message);
    TupleData oldTuple = null;
    boolean oldTupleIsKey = false;
    if (marker == 'K' || marker == 'O') {
      oldTupleIsKey = marker == 'K';
      oldTuple = readTupleData(message);
      marker = readTag(message);
    }
    if (marker != 'N') {
      throw new IllegalStateException("pgoutput update message must finish with a new tuple");
    }
    return new UpdateMessage(relationId, oldTuple, oldTupleIsKey, readTupleData(message));
  }

  private static DeleteMessage decodeDelete(ByteBuffer message) {
    int relationId = message.getInt();
    char marker = readTag(message);
    if (marker != 'K' && marker != 'O') {
      throw new IllegalStateException(
          "pgoutput delete message must provide a key tuple or old tuple");
    }
    return new DeleteMessage(relationId, marker == 'K', readTupleData(message));
  }

  private static TruncateMessage decodeTruncate(ByteBuffer message) {
    int relationCount = message.getInt();
    int options = Byte.toUnsignedInt(message.get());
    List<Integer> relationIds = new ArrayList<>(relationCount);
    for (int index = 0; index < relationCount; index++) {
      relationIds.add(message.getInt());
    }
    return new TruncateMessage(options, List.copyOf(relationIds));
  }

  private static StreamStartMessage decodeStreamStart(ByteBuffer message) {
    return new StreamStartMessage(message.getInt(), Byte.toUnsignedInt(message.get()) == 1);
  }

  private static StreamCommitMessage decodeStreamCommit(ByteBuffer message) {
    int xid = message.getInt();
    byte flags = message.get();
    if (flags != 0) {
      throw new IllegalStateException("pgoutput stream commit flags must currently be zero");
    }
    return new StreamCommitMessage(xid, readLsn(message), readLsn(message), readTimestamp(message));
  }

  private static StreamAbortMessage decodeStreamAbort(ByteBuffer message) {
    return new StreamAbortMessage(message.getInt(), message.getInt());
  }

  private static BeginPrepareMessage decodeBeginPrepare(ByteBuffer message) {
    return new BeginPrepareMessage(
        readLsn(message), readLsn(message), readTimestamp(message), message.getInt(), readString(message));
  }

  private static PrepareMessage decodePrepare(ByteBuffer message) {
    byte flags = message.get();
    if (flags != 0) {
      throw new IllegalStateException("pgoutput prepare flags must currently be zero");
    }
    return new PrepareMessage(
        readLsn(message), readLsn(message), readTimestamp(message), message.getInt(), readString(message));
  }

  private static CommitPreparedMessage decodeCommitPrepared(ByteBuffer message) {
    byte flags = message.get();
    if (flags != 0) {
      throw new IllegalStateException("pgoutput commit prepared flags must currently be zero");
    }
    return new CommitPreparedMessage(
        readLsn(message), readLsn(message), readTimestamp(message), message.getInt(), readString(message));
  }

  private static RollbackPreparedMessage decodeRollbackPrepared(ByteBuffer message) {
    byte flags = message.get();
    if (flags != 0) {
      throw new IllegalStateException("pgoutput rollback prepared flags must currently be zero");
    }
    return new RollbackPreparedMessage(
        readLsn(message), readLsn(message), readTimestamp(message), message.getInt(), readString(message));
  }

  private static TupleData readTupleData(ByteBuffer message) {
    int columnCount = Short.toUnsignedInt(message.getShort());
    List<String> values = new ArrayList<>(columnCount);
    for (int index = 0; index < columnCount; index++) {
      switch (readTag(message)) {
        case 'n' -> values.add(null);
        case 't' -> values.add(readStringBytes(message.getInt(), message));
        case 'u' ->
            throw new IllegalStateException(
                "pgoutput unchanged TOAST values are not supported in the current narrow slice");
        case 'b' ->
            throw new IllegalStateException(
                "pgoutput binary tuple values are not supported; binary mode must remain disabled");
        default ->
            throw new IllegalStateException("pgoutput tuple contained an unknown value marker");
      }
    }
    return new TupleData(Collections.unmodifiableList(new ArrayList<>(values)));
  }

  private static PostgresLsn readLsn(ByteBuffer message) {
    return PostgresLsn.fromLong(message.getLong());
  }

  private static Instant readTimestamp(ByteBuffer message) {
    long microseconds = message.getLong();
    long seconds = Math.floorDiv(microseconds, 1_000_000L);
    long microsRemainder = Math.floorMod(microseconds, 1_000_000L);
    return POSTGRES_EPOCH.plusSeconds(seconds).plusNanos(microsRemainder * 1_000L);
  }

  private static char readTag(ByteBuffer message) {
    return (char) Byte.toUnsignedInt(message.get());
  }

  private static void expectTag(ByteBuffer message, char expected) {
    char actual = readTag(message);
    if (actual != expected) {
      throw new IllegalStateException(
          "pgoutput message expected marker '" + expected + "' but received '" + actual + "'");
    }
  }

  private static String readString(ByteBuffer message) {
    int start = message.position();
    while (message.hasRemaining()) {
      if (message.get() == 0) {
        int end = message.position() - 1;
        int length = end - start;
        byte[] data = new byte[length];
        int current = message.position();
        message.position(start);
        message.get(data);
        message.get();
        message.position(current);
        return new String(data, StandardCharsets.UTF_8);
      }
    }
    throw new IllegalStateException("pgoutput string field was not null terminated");
  }

  private static String readStringBytes(int length, ByteBuffer message) {
    return new String(readBytes(message, length), StandardCharsets.UTF_8);
  }

  private static byte[] readBytes(ByteBuffer message, int length) {
    if (length < 0) {
      throw new IllegalStateException("pgoutput field length must not be negative");
    }
    byte[] data = new byte[length];
    message.get(data);
    return data;
  }

  private static String namespaceOrPgCatalog(String namespace) {
    return namespace.isEmpty() ? "pg_catalog" : namespace;
  }

  public sealed interface DecodedMessage {}

  public record BeginMessage(PostgresLsn finalLsn, Instant commitTimestamp, int transactionId)
      implements DecodedMessage {}

  public record CommitMessage(PostgresLsn commitLsn, PostgresLsn endLsn, Instant commitTimestamp)
      implements DecodedMessage {}

  public record OriginMessage(PostgresLsn originCommitLsn, String originName) implements DecodedMessage {}

  public record LogicalMessage(int flags, PostgresLsn messageLsn, String prefix, byte[] content)
      implements DecodedMessage {}

  public record RelationMessage(
      int relationId,
      String namespace,
      String relationName,
      char replicaIdentity,
      List<RelationColumn> columns)
      implements DecodedMessage {}

  public record RelationColumn(boolean key, String name, int dataTypeOid, int typeModifier) {}

  public record TypeMessage(int dataTypeOid, String namespace, String typeName) implements DecodedMessage {}

  public record InsertMessage(int relationId, TupleData newTuple) implements DecodedMessage {}

  public record UpdateMessage(int relationId, TupleData oldTuple, boolean oldTupleIsKey, TupleData newTuple)
      implements DecodedMessage {}

  public record DeleteMessage(int relationId, boolean oldTupleIsKey, TupleData oldTuple)
      implements DecodedMessage {}

  public record TruncateMessage(int options, List<Integer> relationIds) implements DecodedMessage {}

  public record StreamStartMessage(int transactionId, boolean firstSegment) implements DecodedMessage {}

  public enum StreamStopMessage implements DecodedMessage {
    INSTANCE
  }

  public record StreamCommitMessage(
      int transactionId, PostgresLsn commitLsn, PostgresLsn endLsn, Instant commitTimestamp)
      implements DecodedMessage {}

  public record StreamAbortMessage(int transactionId, int subtransactionId) implements DecodedMessage {}

  public record BeginPrepareMessage(
      PostgresLsn prepareLsn, PostgresLsn endLsn, Instant prepareTimestamp, int transactionId, String gid)
      implements DecodedMessage {}

  public record PrepareMessage(
      PostgresLsn prepareLsn, PostgresLsn endLsn, Instant prepareTimestamp, int transactionId, String gid)
      implements DecodedMessage {}

  public record CommitPreparedMessage(
      PostgresLsn commitLsn, PostgresLsn endLsn, Instant commitTimestamp, int transactionId, String gid)
      implements DecodedMessage {}

  public record RollbackPreparedMessage(
      PostgresLsn rollbackLsn,
      PostgresLsn endLsn,
      Instant rollbackTimestamp,
      int transactionId,
      String gid)
      implements DecodedMessage {}

  public record TupleData(List<String> values) {}
}
