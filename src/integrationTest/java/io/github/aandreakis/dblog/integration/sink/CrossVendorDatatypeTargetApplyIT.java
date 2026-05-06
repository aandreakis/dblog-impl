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
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Tag("integration-docker")
class CrossVendorDatatypeTargetApplyIT {
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
  void coercesMySqlShapedValuesIntoRepresentativePostgresTargetTypes() throws Exception {
    assumeDockerIsAvailable();
    TableId tableId = new TableId("appdb", "public", "typed_values");

    try (PostgreSQLContainer postgres = SharedJdbcTargetContainers.postgres()) {
      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE public.typed_values ("
                + "id BIGINT PRIMARY KEY, "
                + "decimal_value NUMERIC(20,2) NOT NULL, "
                + "binary_value BYTEA NOT NULL, "
                + "timestamp_value TIMESTAMPTZ NOT NULL, "
                + "date_value DATE NOT NULL, "
                + "time_value TIME NOT NULL, "
                + "json_value JSONB NOT NULL, "
                + "enum_value TEXT NOT NULL)");
      }

      try (JdbcApplyChangeEventSink sink =
          JdbcApplyChangeEventSink.forTarget(
              JdbcApplyTargetDialect.POSTGRES,
              postgres.getJdbcUrl(),
              postgres.getUsername(),
              postgres.getPassword(),
              4)) {
        sink.appendEvents(List.of(mysqlShapedInsert(tableId, 1L)));
        sink.appendEvents(List.of(mysqlShapedInsert(tableId, 2L)));
        sink.appendEvents(List.of(mysqlShapedUpdate(tableId, 2L)));
      }

      try (Connection connection =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          Statement statement = connection.createStatement();
          ResultSet resultSet =
              statement.executeQuery(
                  "SELECT id, decimal_value, binary_value, timestamp_value, date_value, time_value, json_value, enum_value "
                      + "FROM public.typed_values ORDER BY id")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getLong("id")).isEqualTo(1L);
        assertThat(resultSet.getBigDecimal("decimal_value")).isEqualByComparingTo("1234567890.12");
        assertThat(resultSet.getBytes("binary_value")).containsExactly(0x12, 0x34);
        assertThat(resultSet.getObject("timestamp_value", OffsetDateTime.class).toInstant())
            .isEqualTo(Instant.parse("2026-03-29T12:34:56Z"));
        assertThat(resultSet.getObject("date_value", LocalDate.class))
            .isEqualTo(LocalDate.parse("2026-03-29"));
        assertThat(resultSet.getObject("time_value", LocalTime.class))
            .isEqualTo(LocalTime.parse("12:34:56"));
        assertThat(normalizeJson(resultSet.getString("json_value")))
            .isEqualTo(normalizeJson("{\"tag\":\"mid\",\"value\":123}"));
        assertThat(resultSet.getString("enum_value")).isEqualTo("mid");

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getLong("id")).isEqualTo(2L);
        assertThat(resultSet.getBigDecimal("decimal_value")).isEqualByComparingTo("2222222222.22");
        assertThat(resultSet.getBytes("binary_value")).containsExactly((byte) 0xAA, 0x01);
        assertThat(resultSet.getObject("timestamp_value", OffsetDateTime.class).toInstant())
            .isEqualTo(Instant.parse("2026-09-09T09:08:07Z"));
        assertThat(resultSet.getObject("date_value", LocalDate.class))
            .isEqualTo(LocalDate.parse("2026-09-09"));
        assertThat(resultSet.getObject("time_value", LocalTime.class))
            .isEqualTo(LocalTime.parse("09:08:07"));
        assertThat(normalizeJson(resultSet.getString("json_value")))
            .isEqualTo(normalizeJson("{\"tag\":\"updated\",\"value\":222}"));
        assertThat(resultSet.getString("enum_value")).isEqualTo("max");

        assertThat(resultSet.next()).isFalse();
      }
    }
  }

  @Test
  void coercesPostgresShapedValuesIntoRepresentativeMySqlTargetTypes() throws Exception {
    assumeDockerIsAvailable();
    TableId tableId = new TableId("appdb", "appdb", "typed_values");

    try (MySQLContainer mysql = SharedJdbcTargetContainers.mysql()) {
      try (Connection connection =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Statement statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE appdb.typed_values ("
                + "id BIGINT PRIMARY KEY, "
                + "uuid_value CHAR(36) NOT NULL, "
                + "xml_value LONGTEXT NOT NULL, "
                + "json_value JSON NOT NULL, "
                + "timestamp_value TIMESTAMP NOT NULL, "
                + "binary_value VARBINARY(255) NOT NULL)");
      }

      try (JdbcApplyChangeEventSink sink =
          JdbcApplyChangeEventSink.forTarget(
              JdbcApplyTargetDialect.MYSQL,
              mysql.getJdbcUrl(),
              mysql.getUsername(),
              mysql.getPassword(),
              4)) {
        sink.appendEvents(List.of(postgresShapedInsert(tableId, 1L)));
        sink.appendEvents(List.of(postgresShapedInsert(tableId, 2L)));
        sink.appendEvents(List.of(postgresShapedUpdate(tableId, 2L)));
      }

      try (Connection connection =
              DriverManager.getConnection(
                  mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
          Statement statement = connection.createStatement();
          ResultSet resultSet =
              statement.executeQuery(
                  "SELECT id, uuid_value, xml_value, json_value, timestamp_value, binary_value "
                      + "FROM appdb.typed_values ORDER BY id")) {
        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getLong("id")).isEqualTo(1L);
        assertThat(resultSet.getString("uuid_value"))
            .isEqualTo("11111111-1111-1111-1111-111111111111");
        assertThat(resultSet.getString("xml_value")).isEqualTo("<root value=\"123\"/>");
        assertThat(normalizeJson(resultSet.getString("json_value")))
            .isEqualTo(normalizeJson("{\"tag\":\"mid\",\"value\":123}"));
        assertThat(resultSet.getTimestamp("timestamp_value").toInstant())
            .isEqualTo(Instant.parse("2026-03-29T12:34:56Z"));
        assertThat(resultSet.getBytes("binary_value")).containsExactly(0x12, 0x34);

        assertThat(resultSet.next()).isTrue();
        assertThat(resultSet.getLong("id")).isEqualTo(2L);
        assertThat(resultSet.getString("uuid_value"))
            .isEqualTo("22222222-2222-2222-2222-222222222222");
        assertThat(resultSet.getString("xml_value"))
            .isEqualTo("<root value=\"222\" tag=\"updated\"/>");
        assertThat(normalizeJson(resultSet.getString("json_value")))
            .isEqualTo(normalizeJson("{\"tag\":\"updated\",\"value\":222}"));
        assertThat(resultSet.getTimestamp("timestamp_value").toInstant())
            .isEqualTo(Instant.parse("2026-09-09T09:08:07Z"));
        assertThat(resultSet.getBytes("binary_value")).containsExactly((byte) 0xAA, 0x01);

        assertThat(resultSet.next()).isFalse();
      }
    }
  }

  private static ChangeEvent mysqlShapedInsert(TableId tableId, long id) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", id);
    afterRow.put("decimal_value", new BigDecimal("1234567890.12"));
    afterRow.put("binary_value", new byte[] {0x12, 0x34});
    afterRow.put("timestamp_value", Instant.parse("2026-03-29T12:34:56Z"));
    afterRow.put("date_value", LocalDate.parse("2026-03-29"));
    afterRow.put("time_value", LocalTime.parse("12:34:56"));
    afterRow.put("json_value", "{\"tag\":\"mid\",\"value\":123}");
    afterRow.put("enum_value", "mid");
    return ChangeEventTestFixtures.fromRowMaps(
        tableId,
        OperationType.INSERT,
        CaptureOrigin.LOG,
        primaryKey,
        null,
        afterRow,
        new OpaqueSourcePosition("mysql-shaped-insert:" + id),
        "tx-" + id,
        null);
  }

  private static ChangeEvent mysqlShapedUpdate(TableId tableId, long id) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> beforeRow = new LinkedHashMap<>();
    beforeRow.put("id", id);
    beforeRow.put("decimal_value", new BigDecimal("1234567890.12"));
    beforeRow.put("binary_value", new byte[] {0x12, 0x34});
    beforeRow.put("timestamp_value", Instant.parse("2026-03-29T12:34:56Z"));
    beforeRow.put("date_value", LocalDate.parse("2026-03-29"));
    beforeRow.put("time_value", LocalTime.parse("12:34:56"));
    beforeRow.put("json_value", "{\"tag\":\"mid\",\"value\":123}");
    beforeRow.put("enum_value", "mid");
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", id);
    afterRow.put("decimal_value", new BigDecimal("2222222222.22"));
    afterRow.put("binary_value", new byte[] {(byte) 0xAA, 0x01});
    afterRow.put("timestamp_value", Instant.parse("2026-09-09T09:08:07Z"));
    afterRow.put("date_value", LocalDate.parse("2026-09-09"));
    afterRow.put("time_value", LocalTime.parse("09:08:07"));
    afterRow.put("json_value", "{\"tag\":\"updated\",\"value\":222}");
    afterRow.put("enum_value", "max");
    return ChangeEventTestFixtures.fromRowMaps(
        tableId,
        OperationType.UPDATE,
        CaptureOrigin.LOG,
        primaryKey,
        beforeRow,
        afterRow,
        new OpaqueSourcePosition("mysql-shaped-update:" + id),
        "tx-" + id,
        null);
  }

  private static ChangeEvent postgresShapedInsert(TableId tableId, long id) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", id);
    afterRow.put("uuid_value", UUID.fromString("11111111-1111-1111-1111-111111111111"));
    afterRow.put("xml_value", "<root value=\"123\"/>");
    afterRow.put("json_value", "{\"tag\":\"mid\",\"value\":123}");
    afterRow.put("timestamp_value", Instant.parse("2026-03-29T12:34:56Z"));
    afterRow.put("binary_value", new byte[] {0x12, 0x34});
    return ChangeEventTestFixtures.fromRowMaps(
        tableId,
        OperationType.INSERT,
        CaptureOrigin.LOG,
        primaryKey,
        null,
        afterRow,
        new OpaqueSourcePosition("postgres-shaped-insert:" + id),
        "tx-" + id,
        null);
  }

  private static ChangeEvent postgresShapedUpdate(TableId tableId, long id) {
    LinkedHashMap<String, Object> primaryKey = new LinkedHashMap<>();
    primaryKey.put("id", id);
    LinkedHashMap<String, Object> beforeRow = new LinkedHashMap<>();
    beforeRow.put("id", id);
    beforeRow.put("uuid_value", UUID.fromString("11111111-1111-1111-1111-111111111111"));
    beforeRow.put("xml_value", "<root value=\"123\"/>");
    beforeRow.put("json_value", "{\"tag\":\"mid\",\"value\":123}");
    beforeRow.put("timestamp_value", Instant.parse("2026-03-29T12:34:56Z"));
    beforeRow.put("binary_value", new byte[] {0x12, 0x34});
    LinkedHashMap<String, Object> afterRow = new LinkedHashMap<>();
    afterRow.put("id", id);
    afterRow.put("uuid_value", UUID.fromString("22222222-2222-2222-2222-222222222222"));
    afterRow.put("xml_value", "<root value=\"222\" tag=\"updated\"/>");
    afterRow.put("json_value", "{\"tag\":\"updated\",\"value\":222}");
    afterRow.put("timestamp_value", Instant.parse("2026-09-09T09:08:07Z"));
    afterRow.put("binary_value", new byte[] {(byte) 0xAA, 0x01});
    return ChangeEventTestFixtures.fromRowMaps(
        tableId,
        OperationType.UPDATE,
        CaptureOrigin.LOG,
        primaryKey,
        beforeRow,
        afterRow,
        new OpaqueSourcePosition("postgres-shaped-update:" + id),
        "tx-" + id,
        null);
  }

  private static String normalizeJson(String value) {
    return value.replaceAll("\\s+", "");
  }

}
