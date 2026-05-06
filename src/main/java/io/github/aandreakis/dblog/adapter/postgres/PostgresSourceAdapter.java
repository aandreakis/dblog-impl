package io.github.aandreakis.dblog.adapter.postgres;

import io.github.aandreakis.dblog.DbLogRuntimeException;
import io.github.aandreakis.dblog.adapter.api.AdapterConnectionSupport;
import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfigValidator;
import io.github.aandreakis.dblog.adapter.api.SourceAdapter;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceConnections;
import io.github.aandreakis.dblog.adapter.api.SourceDialect;
import io.github.aandreakis.dblog.adapter.api.SourcePreflight;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresChunkReader;
import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresHeartbeatTableHelper;
import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresReplicationConnectionFactory;
import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresSourceSchemaInspector;
import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresWatermarkTableHelper;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresLiveRuntimeFactory;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresReplicationConnectionFactory;
import io.github.aandreakis.dblog.adapter.postgres.internal.PostgresSourcePreflight;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.tap.Tap;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class PostgresSourceAdapter implements SourceAdapter {
  public interface Dependencies {
    Connection openSqlConnection(RelationalSourceConfig config);

    Connection openReplicationConnection(RelationalSourceConfig config);

    List<TableSchema> inspectSchemas(Connection connection, RelationalSourceConfig config);

    SourceChunkReader chunkReader();

    WatermarkMetadataWriter watermarkWriter();

    HeartbeatMetadataWriter heartbeatWriter();

    default Optional<OpenedSourceRuntime<? extends SourceTransaction<?>>> maybeOpenLiveRuntime(
        RelationalSourceConfig config,
        RuntimeStateStore stateStore,
        List<TableSchema> contractSchemas,
        PostgresSourceCheckpointStore checkpointStore,
        SourceConnections connections,
        SourceChunkReader chunkReader,
        Tap tap) {
      return Optional.empty();
    }
  }

  private static final Dependencies DEFAULT_DEPENDENCIES =
      new Dependencies() {
        private final JdbcPostgresSourceSchemaInspector inspector = new JdbcPostgresSourceSchemaInspector();
        private final JdbcPostgresChunkReader chunkReader = new JdbcPostgresChunkReader();
        private final JdbcPostgresWatermarkTableHelper watermarkWriter =
            new JdbcPostgresWatermarkTableHelper();
        private final JdbcPostgresHeartbeatTableHelper heartbeatWriter =
            new JdbcPostgresHeartbeatTableHelper();
        private final PostgresLiveRuntimeFactory liveRuntimeFactory =
            new PostgresLiveRuntimeFactory();
        private final PostgresReplicationConnectionFactory replicationConnectionFactory =
            new JdbcPostgresReplicationConnectionFactory();

        @Override
        public Connection openSqlConnection(RelationalSourceConfig config) {
          try {
            return AdapterConnectionSupport.openConfiguredSqlConnection(
                config, PostgresDialect.DISPLAY_NAME);
          } catch (SQLException failure) {
            throw new DbLogRuntimeException(failure);
          }
        }

        @Override
        public Connection openReplicationConnection(RelationalSourceConfig config) {
          String replicationJdbcUrl =
              stringOption(config.options(), "postgres.replicationJdbcUrl", config.jdbcUrl());
          try {
            return replicationConnectionFactory.open(
                replicationJdbcUrl, config.username(), config.password());
          } catch (SQLException failure) {
            throw new DbLogRuntimeException(failure);
          }
        }

        private String stringOption(
            java.util.Map<String, String> options, String key, String defaultValue) {
          String raw = options.get(key);
          if (raw == null || raw.isBlank()) {
            return defaultValue;
          }
          return raw.trim();
        }

        @Override
        public List<TableSchema> inspectSchemas(Connection connection, RelationalSourceConfig config) {
          String databaseName =
              config.databaseName() == null
                  ? RelationalSourceConfigValidator.databaseNameFromJdbcUrl(
                      config.jdbcUrl(), PostgresDialect.JDBC_PREFIX, PostgresDialect.DISPLAY_NAME)
                  : config.databaseName();
          try {
            return inspector.inspectCapturedSchemas(connection, databaseName, config.capturedTables());
          } catch (SQLException failure) {
            throw new DbLogRuntimeException(failure);
          }
        }

        @Override
        public SourceChunkReader chunkReader() {
          return chunkReader;
        }

        @Override
        public WatermarkMetadataWriter watermarkWriter() {
          return watermarkWriter;
        }

        @Override
        public HeartbeatMetadataWriter heartbeatWriter() {
          return heartbeatWriter;
        }

        @Override
        public Optional<OpenedSourceRuntime<? extends SourceTransaction<?>>> maybeOpenLiveRuntime(
            RelationalSourceConfig config,
            RuntimeStateStore stateStore,
            List<TableSchema> contractSchemas,
            PostgresSourceCheckpointStore checkpointStore,
            SourceConnections connections,
            SourceChunkReader chunkReader,
            Tap tap) {
          Connection replicationConnection =
              connections
                  .replication()
                  .orElseThrow(
                      () ->
                          new IllegalStateException(
                              "PostgreSQL live runtime requires a replication Connection in the"
                                  + " SourceConnections bundle"));
          try {
            return Optional.of(
                liveRuntimeFactory.open(
                    config,
                    connections.sql(),
                    replicationConnection,
                    contractSchemas,
                    chunkReader,
                    watermarkWriter(),
                    heartbeatWriter(),
                    checkpointStore,
                    tap));
          } catch (RuntimeException | Error failure) {
            throw failure;
          } catch (Exception failure) {
            throw new DbLogRuntimeException(failure);
          }
        }
      };

  private final Dependencies dependencies;

  public PostgresSourceAdapter() {
    this(DEFAULT_DEPENDENCIES);
  }

  public PostgresSourceAdapter(Dependencies dependencies) {
    this.dependencies = Objects.requireNonNull(dependencies, "dependencies");
  }

  @Override
  public String key() {
    return dialect().key();
  }

  @Override
  public String displayName() {
    return dialect().displayName();
  }

  @Override
  public SourceDialect dialect() {
    return PostgresDialect.INSTANCE;
  }

  public SourceChunkReader chunkReader() {
    return dependencies.chunkReader();
  }

  @Override
  public SourceConnections openConnections(RelationalSourceConfig config) {
    Connection sqlConnection = dependencies.openSqlConnection(config);
    try {
      Connection replicationConnection = dependencies.openReplicationConnection(config);
      return SourceConnections.ofSqlAndReplication(sqlConnection, replicationConnection);
    } catch (RuntimeException | Error failure) {
      try {
        sqlConnection.close();
      } catch (SQLException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }

  @Override
  public SourcePreflight preflight() {
    return new PostgresSourcePreflight();
  }

  @Override
  public List<TableSchema> inspectSchemas(RelationalSourceConfig config) {
    validateSourceConfig(config);
    try (Connection connection = dependencies.openSqlConnection(config)) {
      return dependencies.inspectSchemas(connection, config);
    } catch (SQLException failure) {
      throw new DbLogRuntimeException(failure);
    }
  }

  @Override
  public OpenedSourceRuntime<? extends SourceTransaction<?>> openRuntime(
      RelationalSourceConfig config, RuntimeStateStore stateStore, List<TableSchema> contractSchemas) {
    return openRuntime(config, stateStore, contractSchemas, NoopTap.INSTANCE);
  }

  @Override
  public OpenedSourceRuntime<? extends SourceTransaction<?>> openRuntime(
      RelationalSourceConfig config,
      RuntimeStateStore stateStore,
      List<TableSchema> contractSchemas,
      Tap tap) {
    return openRuntime(
        config,
        stateStore,
        contractSchemas,
        openConnections(config),
        dependencies.chunkReader(),
        tap);
  }

  public OpenedSourceRuntime<? extends SourceTransaction<?>> openRuntime(
      RelationalSourceConfig config,
      RuntimeStateStore stateStore,
      List<TableSchema> contractSchemas,
      SourceConnections connections,
      SourceChunkReader chunkReader,
      Tap tap) {
    validateSourceConfig(config);
    Objects.requireNonNull(stateStore, "stateStore");
    Objects.requireNonNull(contractSchemas, "contractSchemas");
    Objects.requireNonNull(tap, "tap");
    SourceConnections effectiveConnections = Objects.requireNonNull(connections, "connections");
    SourceChunkReader effectiveChunkReader = Objects.requireNonNull(chunkReader, "chunkReader");
    PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);
    try {
      Optional<OpenedSourceRuntime<? extends SourceTransaction<?>>> liveRuntime =
          dependencies.maybeOpenLiveRuntime(
              config,
              stateStore,
              contractSchemas,
              checkpointStore,
              effectiveConnections,
              effectiveChunkReader,
              tap);
      if (liveRuntime.isPresent()) {
        return liveRuntime.orElseThrow();
      }
      // Production must always have a live runtime. The earlier buffered-inspection fallback
      // was a silent-degradation hazard: synthetic LSNs from the buffered path could leak into
      // durable checkpoint state. If no live runtime is available, the operator has a real
      // configuration problem — fail closed so they can see it.
      throw new IllegalStateException(
          "PostgreSQL live runtime factory returned empty — no live runtime dependency is"
              + " configured for the Postgres adapter. Check adapter dependencies and the"
              + " configured source.");
    } catch (RuntimeException | Error failure) {
      try {
        effectiveConnections.close();
      } catch (Exception closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
  }
}
