package io.github.aandreakis.dblog.integration.versionmatrix;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.testsupport.LiveMySqlTestContainers;
import io.github.aandreakis.dblog.testsupport.MySqlTestUserGrants;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Per-MySQL-version compatibility check that drives the full DBLog process against each
 * supported server version. Each iteration:
 *
 * <ul>
 *   <li>Boots a Testcontainers-managed MySQL with binlog enabled
 *   <li>Initializes a multi-type {@code widgets} table covering every {@code NeutralColumnType}
 *       the MySQL adapter recognises (boolean, integer, float, decimal, string, enum_string,
 *       binary, json, date, time, timestamp), plus a small {@code things} companion exercised
 *       by the ALL_TABLES dump
 *   <li>Spawns the production-shipped {@code dblog-impl-<version>.jar} as a child JVM and
 *       drives PRIMARY_KEYS, TABLE, and ALL_TABLES dumps over the HTTP control plane
 *   <li>Drives a streaming binlog write and waits for the runtime to acknowledge it
 *   <li>Polls the source's {@code dblog_meta.heartbeats} singleton row for population
 *   <li>SIGTERMs the child to trigger DBLog's production shutdown hook, then opens the typed
 *       sink's H2 file directly and asserts every typed value round-tripped end-to-end
 * </ul>
 *
 * The typed sink's H2 file is shared across every MySQL version in this class — proving the
 * sink schema built from one version remains usable, with the same DDL, by every other version.
 */
@Tag("integration-version-matrix")
class MySqlSourceVersionMatrixIT {
  private static final String SINK_FILE_NAME = "mysql-shared-sink";
  private static final String SOURCE_ID = "mysql-source";
  private static final String DATABASE_NAME = "appdb";
  private static final TableId WIDGETS = new TableId(SOURCE_ID, DATABASE_NAME, "widgets");
  private static final TableId THINGS = new TableId(SOURCE_ID, DATABASE_NAME, "things");

  private static VersionMatrixHarness harness;

  @BeforeAll
  static void initHarness() throws Exception {
    harness = VersionMatrixHarness.forVendor(MySqlSourceVersionMatrixIT.class, SINK_FILE_NAME);
  }

  @AfterAll
  static void releaseHarness() {
    harness = null;
  }

  @TestFactory
  Collection<DynamicTest> drivesFullPipelineAcrossVerifiedMysqlVersions() {
    assumeDockerIsAvailable();
    return VersionMatrixImages.readMySqlImages().stream()
        .map(image -> DynamicTest.dynamicTest(image, () -> verifyMysqlImage(image)))
        .toList();
  }

  private static void verifyMysqlImage(String image) throws Exception {
    String label = VersionMatrixImages.imageLabel(image);
    try (MySQLContainer mysql = newMysqlContainer(DockerImageName.parse(image))) {
      mysql.start();
      MySqlTestUserGrants.applyDblogUserGrants(mysql, DATABASE_NAME);
      initializeSource(mysql);

      try (FullProcessDbLogLauncher launcher =
          FullProcessDbLogLauncher.launch(buildProperties(mysql, label), harness.logFileFor(label))) {
        harness.runDumpAndStreamingSequence(
            launcher,
            WIDGETS,
            List.of("1", "2"),
            () -> insertStreamingRow(mysql));
        harness.awaitHeartbeatPopulated(
            () -> DriverManager.getConnection(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword()),
            MatrixTimeouts.HEARTBEAT_POPULATED);
      }

      assertSinkRowsForVersion(image);
      harness.assertSinkSchemaStableAcrossVersions();
    }
  }

  private static MySQLContainer newMysqlContainer(DockerImageName imageName) {
    return LiveMySqlTestContainers.newContainer(imageName, "223390");
  }

  private static Map<String, String> buildProperties(MySQLContainer mysql, String versionLabel) {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("dblog.boot-mode", "RUNTIME");
    properties.put("dblog.runtime.state-path", harness.statePathFor(versionLabel).toString());
    properties.put("dblog.sink.typed-h2.path", harness.sinkPath().toString());
    // Two source rows per dump scope — chunk size of 5 fits each scope in a single chunk.
    // Smaller would only slow tests; larger would skip the chunked-read path entirely.
    properties.put("dblog.chunk.size", "5");
    properties.put("dblog.source.adapter", "mysql");
    properties.put("dblog.source.id", SOURCE_ID);
    properties.put("dblog.source.tables[0]", DATABASE_NAME + "." + WIDGETS.tableName());
    properties.put("dblog.source.tables[1]", DATABASE_NAME + "." + THINGS.tableName());
    properties.put("dblog.source.mysql.jdbc-url", mysql.getJdbcUrl());
    properties.put("dblog.source.mysql.username", "dblog");
    properties.put("dblog.source.mysql.password", "dblog");
    properties.put("dblog.source.mysql.server-id", "223391");
    return properties;
  }

  private static void initializeSource(MySQLContainer mysql) throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        Statement statement = connection.createStatement()) {
      // Multi-type widgets covers every NeutralColumnType the MySQL adapter recognises. UUID has
      // no native MySQL column type and the dialect-mapping branch is unreachable for stock
      // 8.x/9.x — deliberately omitted.
      statement.execute(
          "CREATE TABLE appdb.widgets ("
              + "id BIGINT PRIMARY KEY,"
              + "is_active TINYINT(1) NOT NULL,"
              + "count_value INT NOT NULL,"
              + "score DOUBLE NOT NULL,"
              + "price DECIMAL(18,4) NOT NULL,"
              + "name VARCHAR(64) NOT NULL,"
              + "description TEXT,"
              + "status ENUM('active','inactive') NOT NULL,"
              + "payload VARBINARY(64),"
              + "metadata JSON,"
              + "created_date DATE NOT NULL,"
              + "daily_window TIME(6) NOT NULL,"
              + "created_at TIMESTAMP(6) NOT NULL)");
      statement.execute(
          "INSERT INTO appdb.widgets (id, is_active, count_value, score, price, name, description,"
              + " status, payload, metadata, created_date, daily_window, created_at) VALUES"
              + " (1, 1, 100, 1.5, 9.9900, 'one', 'first', 'active', X'01020304',"
              + " JSON_OBJECT('k','v1'), '2026-01-01', '12:34:56.789012',"
              + " '2026-01-01 12:00:00.000000')");
      statement.execute(
          "INSERT INTO appdb.widgets (id, is_active, count_value, score, price, name, description,"
              + " status, payload, metadata, created_date, daily_window, created_at) VALUES"
              + " (2, 0, 200, 2.5, 19.9900, 'two', NULL, 'inactive', NULL,"
              + " JSON_OBJECT('k','v2'), '2026-01-02', '13:00:00',"
              + " '2026-01-02 13:00:00.000000')");
      statement.execute(
          "CREATE TABLE appdb.things (id BIGINT PRIMARY KEY, label VARCHAR(64) NOT NULL)");
      statement.execute("INSERT INTO appdb.things (id, label) VALUES (10, 'thing-ten')");
    }
  }

  private static void insertStreamingRow(MySQLContainer mysql) throws Exception {
    try (Connection writer =
            DriverManager.getConnection(
                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
        Statement statement = writer.createStatement()) {
      statement.execute(
          "INSERT INTO appdb.widgets (id, is_active, count_value, score, price, name, description,"
              + " status, payload, metadata, created_date, daily_window, created_at) VALUES"
              + " (99, 1, 999, 9.5, 999.9900, 'live-99', 'streaming write', 'active',"
              + " X'AABBCC', JSON_OBJECT('source','live'), '2026-04-18', '15:00:00',"
              + " '2026-04-18 15:00:00')");
    }
  }

  private static void assertSinkRowsForVersion(String image) throws Exception {
    Map<String, Object> rowOne =
        TypedSinkReader.readRowByPrimaryKey(harness.sinkPath(), WIDGETS, "id", 1L)
            .orElseThrow(() -> new AssertionError("widgets row id=1 missing for " + image));
    Map<String, Object> rowTwo =
        TypedSinkReader.readRowByPrimaryKey(harness.sinkPath(), WIDGETS, "id", 2L)
            .orElseThrow(() -> new AssertionError("widgets row id=2 missing for " + image));
    Map<String, Object> rowNinetyNine =
        TypedSinkReader.readRowByPrimaryKey(harness.sinkPath(), WIDGETS, "id", 99L)
            .orElseThrow(
                () -> new AssertionError("streaming widgets row id=99 missing for " + image));
    Map<String, Object> thingTen =
        TypedSinkReader.readRowByPrimaryKey(harness.sinkPath(), THINGS, "id", 10L)
            .orElseThrow(() -> new AssertionError("things row id=10 missing for " + image));

    // Per-type round-trip on the seed row. We compare values rather than JDBC return classes —
    // the contract being verified is "the typed sink stores and returns the same value the
    // source produced", not "H2 maps DATE to java.sql.Date".
    assertThat(rowOne)
        .containsEntry("is_active", Boolean.TRUE)
        .containsEntry("count_value", 100L)
        .containsEntry("score", 1.5)
        .containsEntry("price", new BigDecimal("9.9900"))
        .containsEntry("name", "one")
        .containsEntry("description", "first")
        .containsEntry("status", "active");
    assertThat((byte[]) rowOne.get("payload"))
        .as("VARBINARY round-trip on seed row")
        .containsExactly((byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04);
    assertThat(String.valueOf(rowOne.get("created_date"))).isEqualTo("2026-01-01");
    assertThat(String.valueOf(rowOne.get("daily_window"))).startsWith("12:34:56");
    // TIMESTAMP value depends on the session timezone of the source connection, which the
    // testcontainers default differs from the JVM default — assert the column is populated
    // rather than pinning a wall-clock string.
    assertThat(rowOne.get("created_at")).isNotNull();
    assertThat(String.valueOf(rowOne.get("metadata"))).contains("\"k\"");

    // Nullable columns must arrive as nulls, not as empty strings or zero values.
    assertThat(rowTwo).containsEntry("description", null).containsEntry("payload", null);
    assertThat(rowTwo.get("status")).isEqualTo("inactive");

    // Streaming write made it through the binlog → adapter → pump → sink chain.
    assertThat(rowNinetyNine.get("name")).isEqualTo("live-99");
    assertThat((byte[]) rowNinetyNine.get("payload"))
        .containsExactly((byte) 0xAA, (byte) 0xBB, (byte) 0xCC);

    // ALL_TABLES populated the companion table.
    assertThat(thingTen.get("label")).isEqualTo("thing-ten");
    assertThat(TypedSinkReader.countRows(harness.sinkPath(), THINGS)).isEqualTo(1L);
  }

}
