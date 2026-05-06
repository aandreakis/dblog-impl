package io.github.aandreakis.dblog.adapter.postgres.internal;

import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

/**
 * Opens a live PostgreSQL {@code pgoutput} stream through pgJDBC using its public replication
 * API. This matches Debezium's {@code PostgresReplicationConnection} pattern and requires
 * {@code org.postgresql:postgresql} to be on the compile classpath (declared as
 * {@code implementation} in {@code build.gradle}).
 */
public final class JdbcPostgresPgoutputStreamFactory implements PostgresPgoutputStreamFactory {
  @Override
  public PostgresPgoutputStream open(
      Connection replicationConnection, PostgresPgoutputStreamRequest request) throws SQLException {
    Objects.requireNonNull(replicationConnection, "replicationConnection");
    Objects.requireNonNull(request, "request");
    PostgresServerVersion serverVersion = PostgresServerVersion.from(replicationConnection);

    PGConnection pgConnection = replicationConnection.unwrap(PGConnection.class);
    ChainedLogicalStreamBuilder builder =
        pgConnection
            .getReplicationAPI()
            .replicationStream()
            .logical()
            .withSlotName(request.slotName())
            .withSlotOption("proto_version", "1")
            .withSlotOption("publication_names", request.publicationName())
            .withSlotOption("binary", false)
            .withSlotOption("messages", false);
    if (serverVersion.supportsPgoutputOriginOption()) {
      builder = builder.withSlotOption("origin", "none");
    }
    builder =
        builder.withStatusInterval(
            Math.toIntExact(request.statusInterval().toMillis()), TimeUnit.MILLISECONDS);
    if (request.startLsn() != null) {
      builder = builder.withStartPosition(LogSequenceNumber.valueOf(request.startLsn().asLong()));
    }
    return new DirectPostgresPgoutputStream(builder.start());
  }

  /**
   * Direct pgJDBC-backed implementation of {@link PostgresPgoutputStream}. Replaces the earlier
   * reflection-based shim; see {@code JdbcPostgresPgoutputStreamFactory} for the accompanying
   * dependency-shape change.
   */
  private static final class DirectPostgresPgoutputStream implements PostgresPgoutputStream {
    private final PGReplicationStream delegate;

    private DirectPostgresPgoutputStream(PGReplicationStream delegate) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public Optional<ByteBuffer> readPending() throws SQLException {
      ByteBuffer buffer = delegate.readPending();
      return buffer == null ? Optional.empty() : Optional.of(buffer.asReadOnlyBuffer());
    }

    @Override
    public Optional<PostgresLsn> lastReceiveLsn() throws SQLException {
      LogSequenceNumber lsn = delegate.getLastReceiveLSN();
      if (lsn == null || lsn.asLong() == 0L) {
        return Optional.empty();
      }
      return Optional.of(PostgresLsn.fromLong(lsn.asLong()));
    }

    @Override
    public void setAppliedLsn(PostgresLsn lsn) throws SQLException {
      Objects.requireNonNull(lsn, "lsn");
      delegate.setAppliedLSN(LogSequenceNumber.valueOf(lsn.asLong()));
    }

    @Override
    public void setFlushedLsn(PostgresLsn lsn) throws SQLException {
      Objects.requireNonNull(lsn, "lsn");
      delegate.setFlushedLSN(LogSequenceNumber.valueOf(lsn.asLong()));
    }

    @Override
    public void forceUpdateStatus() throws SQLException {
      delegate.forceUpdateStatus();
    }

    @Override
    public void close() throws SQLException {
      delegate.close();
    }
  }
}
