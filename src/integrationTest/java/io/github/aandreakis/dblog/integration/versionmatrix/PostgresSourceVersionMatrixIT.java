package io.github.aandreakis.dblog.integration.versionmatrix;

import static io.github.aandreakis.dblog.testsupport.DockerAvailabilityGate.assumeDockerIsAvailable;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.testsupport.LivePostgresTestContainers;
import io.github.aandreakis.dblog.testsupport.PostgresTestUserGrants;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Per-PostgreSQL-version compatibility check that drives the full DBLog process against each
 * supported server version. Mirrors {@link MySqlSourceVersionMatrixIT} but for PostgreSQL: a
 * multi-type {@code widgets} table covering every {@code NeutralColumnType} the Postgres adapter
 * recognises (boolean, integer, float, decimal, string, enum_string, binary, uuid, xml, date,
 * time, timestamp, json), plus a small {@code things} companion exercised by the ALL_TABLES dump.
 *
 * <p>The typed sink's H2 file is shared across every Postgres version in this class — proving
 * the sink schema built from one version remains usable, with the same DDL, by every other
 * version.
 */
@Tag("integration-version-matrix")
class PostgresSourceVersionMatrixIT {
  private static final String SINK_FILE_NAME = "postgres-shared-sink";
  private static final String SOURCE_ID = "postgres-source";
  private static final String DATABASE_NAME = "appdb";
  private static final String SCHEMA_NAME = "public";
  private static final String RUNTIME_USERNAME = PostgresTestUserGrants.RUNTIME_USERNAME;
  private static final String RUNTIME_PASSWORD = PostgresTestUserGrants.RUNTIME_PASSWORD;
  private static final TableId WIDGETS = new TableId(DATABASE_NAME, SCHEMA_NAME, "widgets");
  private static final TableId THINGS = new TableId(DATABASE_NAME, SCHEMA_NAME, "things");

  private static VersionMatrixHarness harness;

  @BeforeAll
  static void initHarness() throws Exception {
    harness = VersionMatrixHarness.forVendor(PostgresSourceVersionMatrixIT.class, SINK_FILE_NAME);
  }

  @AfterAll
  static void releaseHarness() {
    harness = null;
  }

  @TestFactory
  Collection<DynamicTest> drivesFullPipelineAcrossVerifiedPostgresVersions() {
    assumeDockerIsAvailable();
    return VersionMatrixImages.readPostgresImages().stream()
        .map(image -> DynamicTest.dynamicTest(image, () -> verifyPostgresImage(image)))
        .toList();
  }

  private static void verifyPostgresImage(String image) throws Exception {
    String label = VersionMatrixImages.imageLabel(image);
    try (PostgreSQLContainer postgres = newPostgresContainer(DockerImageName.parse(image))) {
      postgres.start();
      initializeSource(postgres);

      try (FullProcessDbLogLauncher launcher =
          FullProcessDbLogLauncher.launch(
              buildProperties(postgres, label), harness.logFileFor(label))) {
        harness.runDumpAndStreamingSequence(
            launcher,
            WIDGETS,
            List.of("1", "2"),
            () -> insertStreamingRow(postgres));
        harness.awaitHeartbeatPopulated(
            () -> DriverManager.getConnection(
                postgres.getJdbcUrl(), RUNTIME_USERNAME, RUNTIME_PASSWORD),
            MatrixTimeouts.HEARTBEAT_POPULATED);
      }

      assertSinkRowsForVersion(image);
      harness.assertSinkSchemaStableAcrossVersions();
    }
  }

  private static PostgreSQLContainer newPostgresContainer(DockerImageName imageName) {
    return LivePostgresTestContainers.newContainer(imageName);
  }

  private static Map<String, String> buildProperties(
      PostgreSQLContainer postgres, String versionLabel) {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("dblog.boot-mode", "RUNTIME");
    properties.put("dblog.runtime.state-path", harness.statePathFor(versionLabel).toString());
    properties.put("dblog.sink.typed-h2.path", harness.sinkPath().toString());
    // Two source rows per dump scope — chunk size of 5 fits each scope in a single chunk.
    // Smaller would only slow tests; larger would skip the chunked-read path entirely.
    properties.put("dblog.chunk.size", "5");
    properties.put("dblog.source.adapter", "postgres");
    properties.put("dblog.source.id", SOURCE_ID);
    properties.put("dblog.source.tables[0]", SCHEMA_NAME + "." + WIDGETS.tableName());
    properties.put("dblog.source.tables[1]", SCHEMA_NAME + "." + THINGS.tableName());
    properties.put("dblog.source.postgres.jdbc-url", postgres.getJdbcUrl());
    properties.put("dblog.source.postgres.username", RUNTIME_USERNAME);
    properties.put("dblog.source.postgres.password", RUNTIME_PASSWORD);
    properties.put("dblog.source.postgres.publication-name", "vm_pub_widgets");
    properties.put("dblog.source.postgres.slot-name", "vm_slot_widgets");
    return properties;
  }

  private static void initializeSource(PostgreSQLContainer postgres) throws Exception {
    try (Connection connection =
            DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Statement statement = connection.createStatement()) {
      PostgresTestUserGrants.applyDblogRuntimeRole(statement, DATABASE_NAME);
      statement.execute("CREATE TYPE widget_status AS ENUM ('active','inactive')");
      statement.execute(
          "CREATE TABLE public.widgets ("
              + "id BIGINT PRIMARY KEY,"
              + "is_active BOOLEAN NOT NULL,"
              + "count_value INTEGER NOT NULL,"
              + "score DOUBLE PRECISION NOT NULL,"
              + "price NUMERIC(18,4) NOT NULL,"
              + "name VARCHAR(64) NOT NULL,"
              + "description TEXT,"
              + "status widget_status NOT NULL,"
              + "payload BYTEA,"
              + "external_id UUID NOT NULL,"
              + "metadata_xml XML,"
              + "metadata JSONB,"
              + "created_date DATE NOT NULL,"
              + "daily_window TIME NOT NULL,"
              + "created_at TIMESTAMPTZ NOT NULL)");
      statement.execute("ALTER TABLE public.widgets REPLICA IDENTITY FULL");
      statement.execute(
          "INSERT INTO public.widgets (id, is_active, count_value, score, price, name, description,"
              + " status, payload, external_id, metadata_xml, metadata, created_date, daily_window,"
              + " created_at) VALUES "
              + "(1, TRUE, 100, 1.5, 9.9900, 'one', 'first', 'active',"
              + " '\\x01020304'::bytea,"
              + " '00000000-0000-0000-0000-000000000001'::uuid,"
              + " XMLPARSE(CONTENT '<seed id=\"1\"/>'),"
              + " '{\"k\":\"v1\"}'::jsonb,"
              + " '2026-01-01', '12:34:56', '2026-01-01 12:00:00+00')");
      statement.execute(
          "INSERT INTO public.widgets (id, is_active, count_value, score, price, name, description,"
              + " status, payload, external_id, metadata_xml, metadata, created_date, daily_window,"
              + " created_at) VALUES "
              + "(2, FALSE, 200, 2.5, 19.9900, 'two', NULL, 'inactive', NULL,"
              + " '00000000-0000-0000-0000-000000000002'::uuid,"
              + " NULL,"
              + " '{\"k\":\"v2\"}'::jsonb,"
              + " '2026-01-02', '13:00:00', '2026-01-02 13:00:00+00')");
      statement.execute(
          "CREATE TABLE public.things (id BIGINT PRIMARY KEY, label VARCHAR(64) NOT NULL)");
      statement.execute("ALTER TABLE public.things REPLICA IDENTITY FULL");
      statement.execute("INSERT INTO public.things (id, label) VALUES (10, 'thing-ten')");
      statement.execute("ALTER TYPE widget_status OWNER TO " + RUNTIME_USERNAME);
      statement.execute("ALTER TABLE public.widgets OWNER TO " + RUNTIME_USERNAME);
      statement.execute("ALTER TABLE public.things OWNER TO " + RUNTIME_USERNAME);
    }
  }

  private static void insertStreamingRow(PostgreSQLContainer postgres) throws Exception {
    try (Connection writer =
            DriverManager.getConnection(postgres.getJdbcUrl(), RUNTIME_USERNAME, RUNTIME_PASSWORD);
        Statement statement = writer.createStatement()) {
      statement.execute(
          "INSERT INTO public.widgets (id, is_active, count_value, score, price, name, description,"
              + " status, payload, external_id, metadata_xml, metadata, created_date, daily_window,"
              + " created_at) VALUES "
              + "(99, TRUE, 999, 9.5, 999.9900, 'live-99', 'streaming write', 'active',"
              + " '\\xAABBCC'::bytea,"
              + " '00000000-0000-0000-0000-0000000000ff'::uuid,"
              + " XMLPARSE(CONTENT '<live/>'),"
              + " '{\"source\":\"live\"}'::jsonb,"
              + " '2026-04-18', '15:00:00', '2026-04-18 15:00:00+00')");
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

    assertThat(rowOne)
        .containsEntry("is_active", Boolean.TRUE)
        .containsEntry("count_value", 100L)
        .containsEntry("score", 1.5)
        .containsEntry("price", new BigDecimal("9.9900"))
        .containsEntry("name", "one")
        .containsEntry("description", "first")
        .containsEntry("status", "active")
        .containsEntry("external_id", UUID.fromString("00000000-0000-0000-0000-000000000001"));
    assertThat((byte[]) rowOne.get("payload"))
        .as("BYTEA round-trip on seed row")
        .containsExactly((byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04);
    assertThat(String.valueOf(rowOne.get("metadata_xml"))).contains("<seed");
    assertThat(String.valueOf(rowOne.get("created_date"))).isEqualTo("2026-01-01");
    assertThat(String.valueOf(rowOne.get("daily_window"))).startsWith("12:34:56");
    // TIMESTAMPTZ value depends on the session timezone; assert non-null rather than pinning a
    // wall-clock string.
    assertThat(rowOne.get("created_at")).isNotNull();
    assertThat(String.valueOf(rowOne.get("metadata"))).contains("\"k\"");

    assertThat(rowTwo).containsEntry("description", null).containsEntry("payload", null);
    assertThat(rowTwo.get("status")).isEqualTo("inactive");

    assertThat(rowNinetyNine.get("name")).isEqualTo("live-99");
    assertThat((byte[]) rowNinetyNine.get("payload"))
        .containsExactly((byte) 0xAA, (byte) 0xBB, (byte) 0xCC);
    assertThat(rowNinetyNine.get("external_id"))
        .isEqualTo(UUID.fromString("00000000-0000-0000-0000-0000000000ff"));

    assertThat(thingTen.get("label")).isEqualTo("thing-ten");
    assertThat(TypedSinkReader.countRows(harness.sinkPath(), THINGS)).isEqualTo(1L);
  }

}
