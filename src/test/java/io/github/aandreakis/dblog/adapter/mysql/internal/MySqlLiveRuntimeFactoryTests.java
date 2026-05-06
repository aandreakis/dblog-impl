package io.github.aandreakis.dblog.adapter.mysql.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceCheckpointStore;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import io.github.aandreakis.dblog.tap.NoopTap;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MySqlLiveRuntimeFactoryTests {
  @TempDir Path tempDir;

  @Test
  void buildsStreamRequestFromConfigAndPersistsBootstrapResumePositionWhenAbsent()
      throws Exception {
    AtomicReference<MySqlBinlogStreamRequest> capturedRequest = new AtomicReference<>();
    JdbcMySqlSourceSchemaInspector schemaInspector = mock(JdbcMySqlSourceSchemaInspector.class);
    when(schemaInspector.readServerCapabilities(org.mockito.ArgumentMatchers.any()))
        .thenReturn(new MySqlServerCapabilities(true, "ROW", "FULL", true, 0, "FULL"));
    MySqlLiveRuntimeFactory factory =
        new MySqlLiveRuntimeFactory(
            schemaInspector,
            request -> {
              capturedRequest.set(request);
              return new MySqlBinlogStream() {
                @Override
                public java.util.Optional<io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition>
                    connectedPosition() {
                  return java.util.Optional.of(
                      new io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition(
                          "mysql-bin.000001", 41L, null));
                }

                @Override
                public java.util.Optional<MySqlBinlogMessage> readMessage() {
                  return java.util.Optional.empty();
                }

                @Override
                public void close() {}
              };
            });
    TableSchema schema =
        TableSchema.create(
            new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("mysql-live-runtime-factory"));
        Connection connection = DriverManager.getConnection("jdbc:h2:mem:mysql_live_factory;DB_CLOSE_DELAY=-1")) {
      factory.open(
          new io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig(
              "sourceA",
              "jdbc:mysql://127.0.0.1:3307/appdb",
              "dblog",
              "secret",
              "appdb",
              List.of("appdb.customers"),
              java.util.Map.of(
                  "mysql.serverId", "555",
                  "mysql.sourceEventQueueCapacity", "64"),
              false),
          connection,
          List.of(schema),
          new io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader(),
          new io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlWatermarkTableHelper(),
          new io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlHeartbeatTableHelper(),
          new MySqlSourceCheckpointStore(stateStore), NoopTap.INSTANCE);

      assertThat(capturedRequest.get()).isNotNull();
      assertThat(capturedRequest.get().hostname()).isEqualTo("127.0.0.1");
      assertThat(capturedRequest.get().port()).isEqualTo(3307);
      assertThat(capturedRequest.get().databaseName()).isEqualTo("appdb");
      assertThat(capturedRequest.get().serverId()).isEqualTo(555L);
      assertThat(capturedRequest.get().sourceEventQueueCapacity()).isEqualTo(64);
      assertThat(new MySqlSourceCheckpointStore(stateStore).loadBootstrapResumePosition("sourceA"))
          .contains(
              new io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition(
                  "mysql-bin.000001", 41L, null));
    }
  }

  @Test
  void seedsStartPositionFromCurrentBinaryLogStatusWhenNoCheckpointOrBootstrapResumeExists()
      throws Exception {
    AtomicReference<MySqlBinlogStreamRequest> capturedRequest = new AtomicReference<>();
    JdbcMySqlSourceSchemaInspector schemaInspector = mock(JdbcMySqlSourceSchemaInspector.class);
    when(schemaInspector.readServerCapabilities(org.mockito.ArgumentMatchers.any()))
        .thenReturn(new MySqlServerCapabilities(true, "ROW", "FULL", true, 0, "FULL"));
    MySqlLiveRuntimeFactory factory =
        new MySqlLiveRuntimeFactory(
            schemaInspector,
            request -> {
              capturedRequest.set(request);
              return new MySqlBinlogStream() {
                @Override
                public java.util.Optional<io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition>
                    connectedPosition() {
                  return java.util.Optional.empty();
                }

                @Override
                public java.util.Optional<MySqlBinlogMessage> readMessage() {
                  return java.util.Optional.empty();
                }

                @Override
                public void close() {}
              };
            });
    TableSchema schema =
        TableSchema.create(
            new io.github.aandreakis.dblog.core.model.TableId("sourceA", "appdb", "customers"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-11T00:00:00Z"));

    java.sql.Connection sqlConnection = mock(java.sql.Connection.class);
    java.sql.Statement statement = mock(java.sql.Statement.class);
    java.sql.ResultSet resultSet = mock(java.sql.ResultSet.class);
    when(sqlConnection.createStatement()).thenReturn(statement);
    when(statement.executeQuery("SHOW BINARY LOG STATUS")).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(1)).thenReturn("mysql-bin.000009");
    when(resultSet.getLong(2)).thenReturn(98765L);
    when(resultSet.getString(5)).thenReturn("server-1:1-9");

    try (H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("mysql-live-runtime-factory-seeded"))) {
      factory.open(
          new io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig(
              "sourceA",
              "jdbc:mysql://127.0.0.1:3307/appdb",
              "dblog",
              "secret",
              "appdb",
              List.of("appdb.customers"),
              java.util.Map.of(),
              false),
          sqlConnection,
          List.of(schema),
          new io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader(),
          new io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlWatermarkTableHelper(),
          new io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlHeartbeatTableHelper(),
          new MySqlSourceCheckpointStore(stateStore), NoopTap.INSTANCE);

      assertThat(capturedRequest.get().startPosition())
          .isEqualTo(
              new io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition(
                  "mysql-bin.000009", 98765L, "server-1:1-9"));
    }
  }
}
