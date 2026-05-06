package io.github.aandreakis.dblog.integration.adapter.postgres;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.postgres.internal.JdbcPostgresSourceSchemaInspector;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.testsupport.LivePostgresTestContainers;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class JdbcPostgresSourceSchemaInspectorIT {
  @Test
  void inspectsRepresentativePostgresTypesAndMarksUnsupportedColumnsAsIgnored() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = LivePostgresTestContainers.newBaseContainer()) {
      postgres.start();
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        configureSqlConnection(connection);
        statement.execute("CREATE TYPE public.status_enum AS ENUM ('zero', 'mid', 'max')");
        statement.execute(
            "CREATE TABLE public.typed_values ("
                + "id BIGINT PRIMARY KEY, "
                + "uuid_value UUID NOT NULL, "
                + "xml_value XML NOT NULL, "
                + "json_value JSONB NOT NULL, "
                + "binary_value BYTEA NOT NULL, "
                + "timestamp_value TIMESTAMPTZ NOT NULL, "
                + "enum_value public.status_enum NOT NULL, "
                + "geom POINT NULL)");

        TableSchema schema =
            new JdbcPostgresSourceSchemaInspector()
                .inspectCapturedSchemas(connection, postgres.getDatabaseName(), java.util.List.of("public.typed_values"))
                .getFirst();

        Map<String, NeutralColumnType> neutralTypes =
            schema.columns().stream()
                .collect(Collectors.toMap(c -> c.name(), c -> c.neutralType()));

        assertThat(schema.tableId().displayName())
            .isEqualTo(postgres.getDatabaseName() + ".public.typed_values");
        assertThat(schema.primaryKeyColumns()).containsExactly("id");
        assertThat(schema.selectedColumnNames())
            .containsExactly(
                "id",
                "uuid_value",
                "xml_value",
                "json_value",
                "binary_value",
                "timestamp_value",
                "enum_value");
        assertThat(schema.ignoredColumns()).containsExactly("geom");
        assertThat(neutralTypes)
            .containsEntry("id", NeutralColumnType.INTEGER)
            .containsEntry("uuid_value", NeutralColumnType.UUID)
            .containsEntry("xml_value", NeutralColumnType.XML)
            .containsEntry("json_value", NeutralColumnType.JSON)
            .containsEntry("binary_value", NeutralColumnType.BINARY)
            .containsEntry("timestamp_value", NeutralColumnType.TIMESTAMP)
            .containsEntry("enum_value", NeutralColumnType.ENUM_STRING)
            .containsEntry("geom", NeutralColumnType.UNSUPPORTED);
      }
    }
  }

  private static void configureSqlConnection(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
  }

}
