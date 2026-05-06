package io.github.aandreakis.dblog.adapter.mysql.internal;

import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.OpenedSourceRuntime;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfigValidator;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.mysql.MySqlBinlogTransaction;
import io.github.aandreakis.dblog.adapter.mysql.MySqlDialect;
import io.github.aandreakis.dblog.adapter.mysql.MySqlLiveStreamingRuntime;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceCheckpointStore;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.tap.Tap;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Builds the first real stream-fed MySQL runtime path. */
public final class MySqlLiveRuntimeFactory {
  private final JdbcMySqlSourceSchemaInspector schemaInspector;
  private final MySqlBinlogStreamFactory streamFactory;

  public MySqlLiveRuntimeFactory() {
    this(new JdbcMySqlSourceSchemaInspector(), new JdbcMySqlBinlogStreamFactory());
  }

  public MySqlLiveRuntimeFactory(MySqlBinlogStreamFactory streamFactory) {
    this(new JdbcMySqlSourceSchemaInspector(), streamFactory);
  }

  MySqlLiveRuntimeFactory(
      JdbcMySqlSourceSchemaInspector schemaInspector, MySqlBinlogStreamFactory streamFactory) {
    this.schemaInspector = Objects.requireNonNull(schemaInspector, "schemaInspector");
    this.streamFactory = Objects.requireNonNull(streamFactory, "streamFactory");
  }

  public OpenedSourceRuntime<MySqlBinlogTransaction> open(
      RelationalSourceConfig config,
      Connection sqlConnection,
      List<TableSchema> contractSchemas,
      SourceChunkReader chunkReader,
      WatermarkMetadataWriter watermarkWriter,
      HeartbeatMetadataWriter heartbeatWriter,
      MySqlSourceCheckpointStore checkpointStore,
      Tap tap)
      throws Exception {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(sqlConnection, "sqlConnection");
    Objects.requireNonNull(contractSchemas, "contractSchemas");
    Objects.requireNonNull(chunkReader, "chunkReader");
    Objects.requireNonNull(watermarkWriter, "watermarkWriter");
    Objects.requireNonNull(heartbeatWriter, "heartbeatWriter");
    Objects.requireNonNull(checkpointStore, "checkpointStore");
    Objects.requireNonNull(tap, "tap");

    String databaseName =
        config.databaseName() == null
            ? RelationalSourceConfigValidator.databaseNameFromJdbcUrl(
                config.jdbcUrl(), MySqlDialect.JDBC_PREFIX, MySqlDialect.DISPLAY_NAME)
            : config.databaseName();
    schemaInspector.readServerCapabilities(sqlConnection).requireStreamingPrerequisites();
    // Global settings cover user DML; session settings cover DBLog's own watermark writes. Both
    // must agree that row-shaped events reach the binlog.
    schemaInspector.requireSessionBinlogSettings(sqlConnection);
    HostPort hostPort = parseHostPort(config.jdbcUrl());
    MySqlSourcePosition loadedCheckpoint = checkpointStore.load(config.sourceId()).orElse(null);
    MySqlSourcePosition bootstrapResume =
        checkpointStore.loadBootstrapResumePosition(config.sourceId()).orElse(null);
    MySqlSourcePosition startPosition = loadedCheckpoint == null ? bootstrapResume : loadedCheckpoint;
    validateStartPositionAvailability(sqlConnection, startPosition, config.sourceId(), checkpointStore);
    if (startPosition == null) {
      startPosition = currentBinaryLogStatus(sqlConnection);
    }
    boolean useGtidResume = startPosition != null && startPosition.gtidSet() != null;

    MySqlBinlogStreamRequest streamRequest =
        new MySqlBinlogStreamRequest(
            hostPort.host(),
            hostPort.port(),
            databaseName,
            config.username(),
            config.password(),
            longOption(config.options(), "mysql.serverId", 223344L),
            startPosition,
            useGtidResume,
            durationOption(config.options(), "mysql.connectTimeout", Duration.ofSeconds(5)),
            durationOption(config.options(), "mysql.heartbeatInterval", Duration.ofSeconds(5)),
            durationOption(config.options(), "mysql.keepAliveInterval", Duration.ofSeconds(300)),
            durationOption(config.options(), "mysql.netWriteTimeout", Duration.ofMinutes(10)),
            intOption(config.options(), "mysql.sourceEventQueueCapacity", 50_000));

    MySqlBinlogStream stream = streamFactory.open(streamRequest);
    if (bootstrapResume == null && loadedCheckpoint == null) {
      stream.connectedPosition()
          .ifPresent(position -> checkpointStore.saveBootstrapResumePosition(config.sourceId(), position));
    }
    String runId = UUID.randomUUID().toString();
    String sourceStreamId = "mysql-binlog";
    MySqlBinlogSession binlogSession =
        new MySqlBinlogSession(runId, sourceStreamId, config.sourceId(), contractSchemas, stream);
    MySqlTransactionStreamingSession session =
        new MySqlTransactionStreamingSession(
            config.sourceId(), contractSchemas, binlogSession, checkpointStore);
    return new OpenedSourceRuntime<>(
        new MySqlLiveStreamingRuntime(
            runId,
            sqlConnection,
            config.sourceId(),
            sourceStreamId,
            checkpointStore,
            watermarkWriter,
            heartbeatWriter,
            session,
            tap),
        chunkReader,
        loadedCheckpoint == null ? null : loadedCheckpoint.displayValue());
  }

  private static HostPort parseHostPort(String jdbcUrl) {
    String raw = jdbcUrl.substring(MySqlDialect.JDBC_PREFIX.length());
    int paramsIndex = raw.indexOf('?');
    if (paramsIndex >= 0) {
      raw = raw.substring(0, paramsIndex);
    }
    URI uri = URI.create("mysql://" + raw);
    String host = uri.getHost();
    int port = uri.getPort() > 0 ? uri.getPort() : 3306;
    if (host == null || host.isBlank()) {
      throw new IllegalArgumentException("MySQL JDBC URL host could not be parsed: " + jdbcUrl);
    }
    return new HostPort(host, port);
  }

  private static long longOption(Map<String, String> options, String key, long defaultValue) {
    String raw = options.get(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    return Long.parseLong(raw.trim());
  }

  private static int intOption(Map<String, String> options, String key, int defaultValue) {
    String raw = options.get(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    return Integer.parseInt(raw.trim());
  }

  private static Duration durationOption(
      Map<String, String> options, String key, Duration defaultValue) {
    String raw = options.get(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    return Duration.parse(raw.trim());
  }

  private static MySqlSourcePosition currentBinaryLogStatus(Connection sqlConnection) {
    Objects.requireNonNull(sqlConnection, "sqlConnection");
    try (Statement statement = sqlConnection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SHOW BINARY LOG STATUS")) {
        if (resultSet.next()) {
          return sourcePositionFromStatusRow(
              resultSet.getString(1), resultSet.getLong(2), resultSet.getString(5));
        }
      } catch (SQLException ignored) {
        // Fall through to older syntax below.
      }
      try (ResultSet resultSet = statement.executeQuery("SHOW MASTER STATUS")) {
        if (resultSet.next()) {
          return sourcePositionFromStatusRow(
              resultSet.getString(1), resultSet.getLong(2), resultSet.getString(5));
        }
      }
    } catch (SQLException ignored) {
      return null;
    }
    return null;
  }

  private static MySqlSourcePosition sourcePositionFromStatusRow(
      String file, long position, String executedGtidSet) {
    return new MySqlSourcePosition(
        file,
        position,
        executedGtidSet == null || executedGtidSet.isBlank() ? null : executedGtidSet);
  }

  private static void validateStartPositionAvailability(
      Connection sqlConnection,
      MySqlSourcePosition startPosition,
      String sourceId,
      MySqlSourceCheckpointStore checkpointStore)
      throws SQLException {
    if (startPosition == null) {
      return;
    }
    try (Statement statement = sqlConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW BINARY LOGS")) {
      while (resultSet.next()) {
        if (startPosition.binlogFilename().equals(resultSet.getString(1))) {
          return;
        }
      }
    }
    if (startPosition.gtidSet() != null) {
      return;
    }
    String reason =
        "MySQL binlog history no longer contains required checkpoint file "
            + startPosition.binlogFilename()
            + "; full dump required because data loss was detected";
    checkpointStore.saveFullDumpRequiredSignal(sourceId, null, reason);
    throw new IllegalStateException(reason);
  }

  private record HostPort(String host, int port) {}
}
