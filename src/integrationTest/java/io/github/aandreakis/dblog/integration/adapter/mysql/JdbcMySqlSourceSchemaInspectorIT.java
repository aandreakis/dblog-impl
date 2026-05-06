package io.github.aandreakis.dblog.integration.adapter.mysql;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlSourceSchemaInspector;
import io.github.aandreakis.dblog.adapter.mysql.internal.MySqlServerCapabilities;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.testsupport.LiveMySqlTestContainers;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

@Tag("integration-docker")
class JdbcMySqlSourceSchemaInspectorIT {
  @Test
  void readsStreamingServerCapabilitiesFromRealMySql() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql = LiveMySqlTestContainers.newDefaultContainer()) {
      mysql.start();
      try (Connection connection =
          DriverManager.getConnection(mysql.getJdbcUrl(), "root", mysql.getPassword())) {
        MySqlServerCapabilities capabilities =
            new JdbcMySqlSourceSchemaInspector().readServerCapabilities(connection);

        assertThat(capabilities.binaryLoggingEnabled()).isTrue();
        assertThat(capabilities.binlogFormat()).isEqualTo("ROW");
        assertThat(capabilities.binlogRowImage()).isEqualTo("FULL");
        assertThat(capabilities.gtidEnabled()).isTrue();
      }
    }
  }

  @Test
  void inspectsRepresentativeMySqlTypesAndMarksUnsupportedColumnsAsIgnored() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql =
        new MySQLContainer(DockerImageName.parse("mysql:8.4"))
            .withDatabaseName("appdb")
            .withUsername("dblog")
            .withPassword("dblog")
            .withEnv("MYSQL_ROOT_PASSWORD", "root")) {
      mysql.start();
      try (Connection connection =
              DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute(
            "CREATE TABLE appdb.typed_values ("
                + "id BIGINT PRIMARY KEY, "
                + "flag_value BOOLEAN NOT NULL, "
                + "decimal_value DECIMAL(20,2) NOT NULL, "
                + "json_value JSON NOT NULL, "
                + "enum_value ENUM('zero','mid','max') NOT NULL, "
                + "binary_value VARBINARY(16) NOT NULL, "
                + "timestamp_value TIMESTAMP NOT NULL, "
                + "geom GEOMETRY NULL)");

        TableSchema schema =
            new JdbcMySqlSourceSchemaInspector()
                .inspectCapturedSchemas(connection, "sourceA", "appdb", java.util.List.of("appdb.typed_values"))
                .getFirst();

        Map<String, NeutralColumnType> neutralTypes =
            schema.columns().stream()
                .collect(Collectors.toMap(c -> c.name(), c -> c.neutralType()));

        assertThat(schema.tableId().displayName()).isEqualTo("sourceA.appdb.typed_values");
        assertThat(schema.primaryKeyColumns()).containsExactly("id");
        assertThat(schema.selectedColumnNames())
            .containsExactly(
                "id",
                "flag_value",
                "decimal_value",
                "json_value",
                "enum_value",
                "binary_value",
                "timestamp_value");
        assertThat(schema.ignoredColumns()).containsExactly("geom");
        assertThat(neutralTypes)
            .containsEntry("id", NeutralColumnType.INTEGER)
            .containsEntry("flag_value", NeutralColumnType.BOOLEAN)
            .containsEntry("decimal_value", NeutralColumnType.DECIMAL)
            .containsEntry("json_value", NeutralColumnType.JSON)
            .containsEntry("enum_value", NeutralColumnType.ENUM_STRING)
            .containsEntry("binary_value", NeutralColumnType.BINARY)
            .containsEntry("timestamp_value", NeutralColumnType.TIMESTAMP)
            .containsEntry("geom", NeutralColumnType.UNSUPPORTED);
      }
    }
  }

  private static void configureSqlConnection(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

}
