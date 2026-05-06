package io.github.aandreakis.dblog.state.jdbc;

import io.github.aandreakis.dblog.core.request.DumpTableProgress;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTupleCodec;
import io.github.aandreakis.dblog.state.api.DumpProgressRepository;
import java.util.Objects;
import java.util.Optional;

public final class JdbcDumpProgressRepository implements DumpProgressRepository {
  private final JdbcStateStoreSupport jdbc;

  public JdbcDumpProgressRepository(JdbcStateStoreSupport jdbc) {
    this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
  }

  @Override
  public void save(DumpTableProgress progress) {
    Objects.requireNonNull(progress, "progress");
    jdbc.withTransaction(
        connection -> {
          int updated =
              jdbc.update(
                  connection,
                  "UPDATE DUMP_TABLE_PROGRESS SET SCHEMA_FINGERPRINT = ?, REQUEST_UPPER_BOUND_PRIMARY_KEY = ?, LAST_COMPLETED_PRIMARY_KEY = ?, ACTIVE_CHUNK_START_AFTER = ?, ACTIVE_LOW_WATERMARK = ?, ACTIVE_HIGH_WATERMARK = ?, CHUNK_COMPLETED = ? WHERE JOB_ID = ? AND TABLE_NAME_VALUE = ?",
                  progress.schemaFingerprint(),
                  PrimaryKeyTupleCodec.encodeNullable(progress.requestUpperBoundPrimaryKeyTuple()),
                  PrimaryKeyTupleCodec.encodeNullable(progress.lastCompletedPrimaryKeyTuple()),
                  PrimaryKeyTupleCodec.encodeNullable(progress.activeChunkStartAfterTuple()),
                  progress.activeLowWatermark(),
                  progress.activeHighWatermark(),
                  progress.chunkCompleted() ? 1 : 0,
                  progress.jobId(),
                  progress.tableName());
          if (updated == 0) {
            jdbc.insert(
                connection,
                "INSERT INTO DUMP_TABLE_PROGRESS (JOB_ID, TABLE_NAME_VALUE, SCHEMA_FINGERPRINT, REQUEST_UPPER_BOUND_PRIMARY_KEY, LAST_COMPLETED_PRIMARY_KEY, ACTIVE_CHUNK_START_AFTER, ACTIVE_LOW_WATERMARK, ACTIVE_HIGH_WATERMARK, CHUNK_COMPLETED) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                progress.jobId(),
                progress.tableName(),
                progress.schemaFingerprint(),
                PrimaryKeyTupleCodec.encodeNullable(progress.requestUpperBoundPrimaryKeyTuple()),
                PrimaryKeyTupleCodec.encodeNullable(progress.lastCompletedPrimaryKeyTuple()),
                PrimaryKeyTupleCodec.encodeNullable(progress.activeChunkStartAfterTuple()),
                progress.activeLowWatermark(),
                progress.activeHighWatermark(),
                progress.chunkCompleted() ? 1 : 0);
          }
          return null;
        });
  }

  @Override
  public Optional<DumpTableProgress> load(String jobId, String tableName) {
    Objects.requireNonNull(jobId, "jobId");
    Objects.requireNonNull(tableName, "tableName");
    return jdbc.withTransaction(
        connection ->
            jdbc.queryOptional(
                connection,
                "SELECT SCHEMA_FINGERPRINT, REQUEST_UPPER_BOUND_PRIMARY_KEY, LAST_COMPLETED_PRIMARY_KEY, ACTIVE_CHUNK_START_AFTER, ACTIVE_LOW_WATERMARK, ACTIVE_HIGH_WATERMARK, CHUNK_COMPLETED FROM DUMP_TABLE_PROGRESS WHERE JOB_ID = ? AND TABLE_NAME_VALUE = ?",
                resultSet ->
                    new DumpTableProgress(
                        jobId,
                        tableName,
                        resultSet.getString("SCHEMA_FINGERPRINT"),
                        PrimaryKeyTupleCodec.decodeNullable(
                            resultSet.getString("REQUEST_UPPER_BOUND_PRIMARY_KEY")),
                        PrimaryKeyTupleCodec.decodeNullable(
                            resultSet.getString("LAST_COMPLETED_PRIMARY_KEY")),
                        PrimaryKeyTupleCodec.decodeNullable(
                            resultSet.getString("ACTIVE_CHUNK_START_AFTER")),
                        resultSet.getString("ACTIVE_LOW_WATERMARK"),
                        resultSet.getString("ACTIVE_HIGH_WATERMARK"),
                        resultSet.getInt("CHUNK_COMPLETED") != 0),
                jobId,
                tableName));
  }

  @Override
  public void delete(String jobId, String tableName) {
    Objects.requireNonNull(jobId, "jobId");
    Objects.requireNonNull(tableName, "tableName");
    jdbc.withTransaction(
        connection -> {
          jdbc.update(
              connection,
              "DELETE FROM DUMP_TABLE_PROGRESS WHERE JOB_ID = ? AND TABLE_NAME_VALUE = ?",
              jobId,
              tableName);
          return null;
        });
  }

  @Override
  public void deleteAll() {
    jdbc.withTransaction(
        connection -> {
          deleteAllInTxn(connection);
          return null;
        });
  }

  /**
   * In-transaction variant of {@link #deleteAll()} for composite atomic invalidations.
   */
  void deleteAllInTxn(java.sql.Connection connection) throws java.sql.SQLException {
    java.util.Objects.requireNonNull(connection, "connection");
    jdbc.update(connection, "DELETE FROM DUMP_TABLE_PROGRESS");
  }
}
