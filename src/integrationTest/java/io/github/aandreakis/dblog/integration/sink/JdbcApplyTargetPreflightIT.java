package io.github.aandreakis.dblog.integration.sink;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.sink.jdbc.JdbcApplyTargetDialect;
import io.github.aandreakis.dblog.sink.jdbc.JdbcApplyTargetPreflight;
import io.github.aandreakis.dblog.sink.jdbc.TargetApplyContractException;
import io.github.aandreakis.dblog.sink.jdbc.TargetApplyFailureType;
import io.github.aandreakis.dblog.testsupport.SharedJdbcTargetContainers;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class JdbcApplyTargetPreflightIT {
  @BeforeAll
  static void startSharedTargets() {
    assumeDockerIsAvailable();
    SharedJdbcTargetContainers.start();
  }

  @BeforeEach
  void resetSharedTargets() throws Exception {
    SharedJdbcTargetContainers.resetMySql();
    SharedJdbcTargetContainers.resetPostgres();
  }

  @Test
  void reportsStructuredMissingTableFailureForMySqlTargets() throws Exception {
    assumeDockerIsAvailable();

    try (MySQLContainer mysql = SharedJdbcTargetContainers.mysql()) {
      TableSchema capturedSchema =
          TableSchema.create(
              new TableId("source", "appdb", "sample_orders"),
              List.of(
                  new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, 1, false),
                  new ColumnDefinition(
                      "customer_name",
                      "varchar(255)",
                      NeutralColumnType.STRING,
                      false,
                      0,
                      true)),
              Instant.EPOCH);

      assertThatThrownBy(
              () ->
                  JdbcApplyTargetPreflight.validate(
                      JdbcApplyTargetDialect.MYSQL,
                      mysql.getJdbcUrl(),
                      mysql.getUsername(),
                      mysql.getPassword(),
                      Duration.ofSeconds(2),
                      List.of(capturedSchema)))
          .isInstanceOf(TargetApplyContractException.class)
          .satisfies(
              failure ->
                  assertThat(((TargetApplyContractException) failure).failure().type())
                      .isEqualTo(TargetApplyFailureType.TARGET_TABLE_MISSING))
          .hasMessageContaining("Target table does not exist")
          .hasMessageContaining("sample_orders");
    }
  }

  @Test
  void postgresPreflightRespectsExactCaseForQuotedCaseDistinctColumns() throws Exception {
    assumeDockerIsAvailable();

    try (PostgreSQLContainer postgres = SharedJdbcTargetContainers.postgres()) {
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE public.case_sensitive_orders ("
                + "\"Id\" BIGINT PRIMARY KEY, "
                + "\"id\" TEXT, "
                + "\"customer_name\" TEXT NOT NULL)");
      }

      TableSchema capturedSchema =
          TableSchema.create(
              new TableId("source", "public", "case_sensitive_orders"),
              List.of(
                  new ColumnDefinition("Id", "bigint", NeutralColumnType.INTEGER, true, 1, false),
                  new ColumnDefinition(
                      "customer_name", "text", NeutralColumnType.STRING, false, 0, false)),
              Instant.EPOCH);

      JdbcApplyTargetPreflight.validate(
          JdbcApplyTargetDialect.POSTGRES,
          postgres.getJdbcUrl(),
          postgres.getUsername(),
          postgres.getPassword(),
          Duration.ofSeconds(2),
          List.of(capturedSchema));
    }
  }

}
