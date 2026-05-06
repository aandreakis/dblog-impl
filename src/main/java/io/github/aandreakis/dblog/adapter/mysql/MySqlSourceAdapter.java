package io.github.aandreakis.dblog.adapter.mysql;

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
import io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader;
import io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlHeartbeatTableHelper;
import io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlSourceSchemaInspector;
import io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlWatermarkTableHelper;
import io.github.aandreakis.dblog.adapter.mysql.internal.MySqlLiveRuntimeFactory;
import io.github.aandreakis.dblog.adapter.mysql.internal.MySqlSourcePreflight;
import io.github.aandreakis.dblog.adapter.mysql.internal.MySqlTransactionStreamingSession;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.tap.Tap;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class MySqlSourceAdapter implements SourceAdapter {
  public interface Dependencies {
    Connection openSqlConnection(RelationalSourceConfig config);

    List<TableSchema> inspectSchemas(Connection connection, RelationalSourceConfig config);

    SourceChunkReader chunkReader();

    WatermarkMetadataWriter watermarkWriter();

    HeartbeatMetadataWriter heartbeatWriter();

    default Optional<OpenedSourceRuntime<? extends SourceTransaction<?>>> maybeOpenLiveRuntime(
        RelationalSourceConfig config,
        RuntimeStateStore stateStore,
        List<TableSchema> contractSchemas,
        MySqlSourceCheckpointStore checkpointStore,
        SourceConnections connections,
        SourceChunkReader chunkReader,
        Tap tap) {
      return Optional.empty();
    }
  }

  private static final Dependencies DEFAULT_DEPENDENCIES =
      new Dependencies() {
        private final JdbcMySqlSourceSchemaInspector inspector = new JdbcMySqlSourceSchemaInspector();
        private final JdbcMySqlChunkReader chunkReader = new JdbcMySqlChunkReader();
        private final JdbcMySqlWatermarkTableHelper watermarkWriter =
            new JdbcMySqlWatermarkTableHelper();
        private final JdbcMySqlHeartbeatTableHelper heartbeatWriter =
            new JdbcMySqlHeartbeatTableHelper();
        private final MySqlLiveRuntimeFactory liveRuntimeFactory = new MySqlLiveRuntimeFactory();

        @Override
        public Connection openSqlConnection(RelationalSourceConfig config) {
          try {
            return AdapterConnectionSupport.openConfiguredSqlConnection(
                config, MySqlDialect.DISPLAY_NAME);
          } catch (SQLException failure) {
            throw new DbLogRuntimeException(failure);
          }
        }

        @Override
        public List<TableSchema> inspectSchemas(Connection connection, RelationalSourceConfig config) {
          String jdbcDatabase =
              RelationalSourceConfigValidator.databaseNameFromJdbcUrl(
                  config.jdbcUrl(), MySqlDialect.JDBC_PREFIX, MySqlDialect.DISPLAY_NAME);
          String effectiveDatabase = config.databaseName() == null ? jdbcDatabase : config.databaseName();
          try {
            return inspector.inspectCapturedSchemas(
                connection, config.sourceId(), effectiveDatabase, config.capturedTables());
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
            MySqlSourceCheckpointStore checkpointStore,
            SourceConnections connections,
            SourceChunkReader chunkReader,
            Tap tap) {
          try {
            return Optional.of(
                liveRuntimeFactory.open(
                    config,
                    connections.sql(),
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

  public MySqlSourceAdapter() {
    this(DEFAULT_DEPENDENCIES);
  }

  public MySqlSourceAdapter(Dependencies dependencies) {
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
    return MySqlDialect.INSTANCE;
  }

  public SourceChunkReader chunkReader() {
    return dependencies.chunkReader();
  }

  @Override
  public SourceConnections openConnections(RelationalSourceConfig config) {
    return SourceConnections.ofSql(dependencies.openSqlConnection(config));
  }

  @Override
  public SourcePreflight preflight() {
    return new MySqlSourcePreflight();
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
    MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);
    MySqlSourcePosition loadedCheckpoint = checkpointStore.load(config.sourceId()).orElse(null);
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
      return new OpenedSourceRuntime<>(
          new MySqlBufferedStreamingRuntime(
              effectiveConnections.sql(),
              config.sourceId(),
              "mysql-inspection",
              contractSchemas,
              dependencies.watermarkWriter(),
              dependencies.heartbeatWriter(),
              checkpoint -> checkpointStore.save(config.sourceId(), checkpoint),
              loadedCheckpoint,
              tap),
          effectiveChunkReader,
          loadedCheckpoint == null ? null : loadedCheckpoint.displayValue());
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
