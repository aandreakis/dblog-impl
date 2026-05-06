package io.github.aandreakis.dblog.integration.sink;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.sink.jdbc.JdbcApplyChangeEventSink;
import io.github.aandreakis.dblog.sink.jdbc.JdbcApplyTargetDialect;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import io.github.aandreakis.dblog.testsupport.SharedJdbcTargetContainers;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class JdbcApplyChangeEventSinkIT {
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
  void convergesUnderReplayWithPostgresDialectAgainstRealJdbcTarget() throws Exception {
    assumeDockerIsAvailable();
    TableId tableId = new TableId("appdb", "public", "sample_orders");

    try (PostgreSQLContainer postgres = SharedJdbcTargetContainers.postgres()) {
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE public.sample_orders ("
                + "id BIGINT PRIMARY KEY, "
                + "customer_name TEXT NOT NULL, "
                + "status TEXT NOT NULL)");
      }

      try (JdbcApplyChangeEventSink sink =
          JdbcApplyChangeEventSink.forTarget(
              JdbcApplyTargetDialect.POSTGRES,
              postgres.getJdbcUrl(),
              postgres.getUsername(),
              postgres.getPassword(),
              4)) {
        sink.appendEvents(List.of(upsertEvent(tableId, CaptureOrigin.LOG, 1L, "alice", "NEW")));
        sink.appendEvents(List.of(upsertEvent(tableId, CaptureOrigin.LOG, 1L, "alice", "DONE")));
        sink.appendEvents(List.of(upsertEvent(tableId, CaptureOrigin.LOG, 1L, "alice", "DONE")));
      }

      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement();
          ResultSet resultSet =
              statement.executeQuery(
                  "SELECT customer_name, status, COUNT(*) OVER() FROM public.sample_orders WHERE id = 1")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString(1)).isEqualTo("alice");
        assertThat(resultSet.getString(2)).isEqualTo("DONE");
        assertThat(resultSet.getInt(3)).isEqualTo(1);
      }
    }
  }

  @Test
  void convergesUnderReplayWithMySqlDialectAgainstRealJdbcTarget() throws Exception {
    assumeDockerIsAvailable();
    TableId tableId = new TableId("appdb", "appdb", "sample_orders");

    try (MySQLContainer mysql = SharedJdbcTargetContainers.mysql()) {
      try (Connection connection =
              DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Statement statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE appdb.sample_orders ("
                + "id BIGINT PRIMARY KEY, "
                + "customer_name VARCHAR(255) NOT NULL, "
                + "status VARCHAR(64) NOT NULL)");
      }

      try (JdbcApplyChangeEventSink sink =
          JdbcApplyChangeEventSink.forTarget(
              JdbcApplyTargetDialect.MYSQL,
              mysql.getJdbcUrl(),
              mysql.getUsername(),
              mysql.getPassword(),
              4)) {
        sink.appendEvents(List.of(upsertEvent(tableId, CaptureOrigin.LOG, 1L, "bob", "NEW")));
        sink.appendEvents(List.of(upsertEvent(tableId, CaptureOrigin.SELECT, 1L, "bob", "DONE")));
        sink.appendEvents(List.of(upsertEvent(tableId, CaptureOrigin.SELECT, 1L, "bob", "DONE")));
      }

      try (Connection connection =
              DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Statement statement = connection.createStatement();
          ResultSet resultSet =
              statement.executeQuery(
                  "SELECT customer_name, status, COUNT(*) OVER() FROM appdb.sample_orders WHERE id = 1")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString(1)).isEqualTo("bob");
        assertThat(resultSet.getString(2)).isEqualTo("DONE");
        assertThat(resultSet.getInt(3)).isEqualTo(1);
      }
    }
  }

  @Test
  void coercesNumericAndTimestampValuesIntoTextColumnsOnPostgresTarget() throws Exception {
    assumeDockerIsAvailable();
    TableId tableId = new TableId("appdb", "public", "sample_migrations");

    try (PostgreSQLContainer postgres = SharedJdbcTargetContainers.postgres()) {
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE public.sample_migrations ("
                + "id BIGINT PRIMARY KEY, "
                + "numeric_as_text TEXT NOT NULL, "
                + "timestamp_as_text TEXT NOT NULL)");
      }

      try (JdbcApplyChangeEventSink sink =
          JdbcApplyChangeEventSink.forTarget(
              JdbcApplyTargetDialect.POSTGRES,
              postgres.getJdbcUrl(),
              postgres.getUsername(),
              postgres.getPassword(),
              4)) {
        sink.appendEvents(List.of(coercionEvent(tableId, 1L)));
      }

      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement();
          ResultSet resultSet =
              statement.executeQuery(
                  "SELECT numeric_as_text, timestamp_as_text FROM public.sample_migrations WHERE id = 1")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString(1)).isEqualTo("42");
        assertThat(resultSet.getString(2)).isEqualTo("2026-03-29T12:34:56Z");
      }
    }
  }

  @Test
  void coercesNumericAndTimestampValuesIntoTextColumnsOnMySqlTarget() throws Exception {
    assumeDockerIsAvailable();
    TableId tableId = new TableId("appdb", "appdb", "sample_migrations");

    try (MySQLContainer mysql = SharedJdbcTargetContainers.mysql()) {
      try (Connection connection =
              DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Statement statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE appdb.sample_migrations ("
                + "id BIGINT PRIMARY KEY, "
                + "numeric_as_text VARCHAR(255) NOT NULL, "
                + "timestamp_as_text VARCHAR(255) NOT NULL)");
      }

      try (JdbcApplyChangeEventSink sink =
          JdbcApplyChangeEventSink.forTarget(
              JdbcApplyTargetDialect.MYSQL,
              mysql.getJdbcUrl(),
              mysql.getUsername(),
              mysql.getPassword(),
              4)) {
        sink.appendEvents(List.of(coercionEvent(tableId, 1L)));
      }

      try (Connection connection =
              DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Statement statement = connection.createStatement();
          ResultSet resultSet =
              statement.executeQuery(
                  "SELECT numeric_as_text, timestamp_as_text FROM appdb.sample_migrations WHERE id = 1")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getString(1)).isEqualTo("42");
        assertThat(resultSet.getString(2)).isEqualTo("2026-03-29T12:34:56Z");
      }
    }
  }

  private static ChangeEvent upsertEvent(
      TableId tableId, CaptureOrigin origin, long id, String customerName, String status) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", id);
    afterRow.put("customer_name", customerName);
    afterRow.put("status", status);
    return ChangeEventTestFixtures.fromRowMaps(
        tableId,
        OperationType.UPDATE,
        origin,
        primaryKey,
        null,
        afterRow,
        new OpaqueSourcePosition("pos:" + id + ":" + status),
        "tx-" + id,
        null);
  }

  private static ChangeEvent coercionEvent(TableId tableId, long id) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", id);
    afterRow.put("numeric_as_text", 42L);
    afterRow.put("timestamp_as_text", Instant.parse("2026-03-29T12:34:56Z"));
    return ChangeEventTestFixtures.fromRowMaps(
        tableId,
        OperationType.INSERT,
        CaptureOrigin.LOG,
        primaryKey,
        null,
        afterRow,
        new OpaqueSourcePosition("coercion-pos:" + id),
        "tx-" + id,
        null);
  }

}
