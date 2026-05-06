package io.github.aandreakis.dblog.e2e.support;

import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceAdapter;
import io.github.aandreakis.dblog.adapter.postgres.PostgresSourceCheckpointStore;
import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresChunkReader;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.runtime.bootstrap.InspectionOnlySourceRuntime;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Test-only {@link PostgresSourceAdapter.Dependencies} factory that wires
 * {@link InspectionOnlySourceRuntime} as the live runtime. Lets H2-backed e2e tests exercise
 * the dump/reconcile/sink path without starting a real PostgreSQL container.
 *
 * <p>Production must never use this. {@link PostgresSourceAdapter} intentionally fails
 * closed when no live runtime dependency is configured (see its openRuntime javadoc) because
 * the earlier implicit buffered-inspection fallback could leak synthetic LSNs into durable
 * checkpoint state.
 *
 * <p>The MySQL adapter still ships a buffered-inspection fallback (see
 * {@code MySqlSourceAdapter.openRuntime}) so its e2e tests do not need an equivalent helper.
 */
public final class PostgresInspectionOnlyDependencies {
  private PostgresInspectionOnlyDependencies() {}

  /**
   * Build a {@link PostgresSourceAdapter.Dependencies} backed by the given H2 JDBC URL,
   * contract schema, and no-op metadata writers. Uses the real {@link JdbcPostgresChunkReader}
   * which works against H2 because H2's PostgreSQL mode understands the SQL the chunk reader
   * emits. Every {@code openSqlConnection} call opens a fresh connection on the same JDBC
   * URL so the helper is reusable across restart tests.
   */
  public static PostgresSourceAdapter.Dependencies withFixedSchema(
      String jdbcUrl, TableSchema schema) {
    Objects.requireNonNull(jdbcUrl, "jdbcUrl");
    Objects.requireNonNull(schema, "schema");
    return withFixedSchemas(jdbcUrl, List.of(schema), new JdbcPostgresChunkReader());
  }

  /**
   * Variant that returns multiple contract schemas from {@code inspectSchemas}. Useful for
   * tests that exercise multi-table dumps.
   */
  public static PostgresSourceAdapter.Dependencies withFixedSchemas(
      String jdbcUrl, List<TableSchema> schemas) {
    return withFixedSchemas(jdbcUrl, schemas, new JdbcPostgresChunkReader());
  }

  /**
   * Variant that lets the caller supply a custom {@link SourceChunkReader}. Failure-mode
   * tests use this to throw {@link io.github.aandreakis.dblog.core.schema.SchemaDriftException},
   * simulate source outages, or inject primary-key selection failures while still reusing
   * the InspectionOnlySourceRuntime wiring.
   */
  public static PostgresSourceAdapter.Dependencies withFixedSchema(
      String jdbcUrl, TableSchema schema, SourceChunkReader chunkReader) {
    Objects.requireNonNull(jdbcUrl, "jdbcUrl");
    Objects.requireNonNull(schema, "schema");
    return withFixedSchemas(jdbcUrl, List.of(schema), chunkReader);
  }

  public static PostgresSourceAdapter.Dependencies withFixedSchemas(
      String jdbcUrl, List<TableSchema> schemas, SourceChunkReader chunkReader) {
    Objects.requireNonNull(jdbcUrl, "jdbcUrl");
    Objects.requireNonNull(chunkReader, "chunkReader");
    List<TableSchema> snapshot = List.copyOf(schemas);
    return new PostgresSourceAdapter.Dependencies() {
      @Override
      public Connection openSqlConnection(RelationalSourceConfig config) {
        try {
          return DriverManager.getConnection(jdbcUrl);
        } catch (java.sql.SQLException failure) {
          throw new io.github.aandreakis.dblog.DbLogRuntimeException(failure);
        }
      }

      @Override
      public Connection openReplicationConnection(RelationalSourceConfig config) {
        // Inspection-only e2e tests never activate a live replication stream; the live-runtime
        // path is bypassed by maybeOpenLiveRuntime below. Hand out an ordinary SQL connection so
        // the adapter's SourceConnections bundle is well-formed without requiring a real
        // replication listener on the fixture.
        try {
          return DriverManager.getConnection(jdbcUrl);
        } catch (java.sql.SQLException failure) {
          throw new io.github.aandreakis.dblog.DbLogRuntimeException(failure);
        }
      }

      @Override
      public List<TableSchema> inspectSchemas(Connection connection, RelationalSourceConfig config) {
        return snapshot;
      }

      @Override
      public SourceChunkReader chunkReader() {
        return chunkReader;
      }

      @Override
      public WatermarkMetadataWriter watermarkWriter() {
        return NoopWatermarkWriter.INSTANCE;
      }

      @Override
      public HeartbeatMetadataWriter heartbeatWriter() {
        return NoopHeartbeatWriter.INSTANCE;
      }

      @Override
      public Optional<OpenedSourceRuntime<? extends SourceTransaction<?>>> maybeOpenLiveRuntime(
          RelationalSourceConfig config,
          RuntimeStateStore stateStore,
          List<TableSchema> contractSchemas,
          PostgresSourceCheckpointStore checkpointStore,
          io.github.aandreakis.dblog.adapter.api.SourceConnections connections,
          SourceChunkReader effectiveChunkReader,
          io.github.aandreakis.dblog.tap.Tap tap) {
        return Optional.of(
            new OpenedSourceRuntime<>(
                new InspectionOnlySourceRuntime(
                    "PostgreSQL",
                    connections.sql(),
                    contractSchemas,
                    "postgresql",
                    null,
                    null,
                    1024,
                    tap),
                effectiveChunkReader,
                null));
      }
    };
  }

  private enum NoopWatermarkWriter implements WatermarkMetadataWriter {
    INSTANCE;

    @Override
    public void ensureMetadataTable(Connection connection) {}

    @Override
    public void writeWatermark(
        Connection sqlConnection,
        String runId,
        io.github.aandreakis.dblog.core.model.WatermarkToken token) {}
  }

  private enum NoopHeartbeatWriter implements HeartbeatMetadataWriter {
    INSTANCE;

    @Override
    public void ensureHeartbeatTable(Connection connection) {}

    @Override
    public boolean writeHeartbeatIfDue(
        Connection connection,
        String runId,
        String sourceStreamId,
        Instant heartbeatTime,
        Duration minimumInterval) {
      return false;
    }
  }
}
