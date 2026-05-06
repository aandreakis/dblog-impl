package io.github.aandreakis.dblog.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.api.HeartbeatMetadataWriter;
import io.github.aandreakis.dblog.adapter.api.RelationalSourceConfig;
import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.WatermarkMetadataWriter;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourceAdapter;
import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.runtime.bootstrap.DbLogApplication;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InspectionModeMySqlTargetedRepairFlowTests {
  private static final Pattern REQUEST_ID_PATTERN =
      Pattern.compile("\"requestId\"\\s*:\\s*\"([^\"]+)\"");

  @TempDir Path tempDir;

  @Test
  void processesATargetedRepairRequestAndReportsMissingKeys() throws Exception {
    String h2Jdbc = "jdbc:h2:mem:phase30_mysql_targeted;MODE=MySQL;DB_CLOSE_DELAY=-1";
    try (Connection connection = DriverManager.getConnection(h2Jdbc)) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE SCHEMA IF NOT EXISTS appdb");
        statement.execute("CREATE TABLE appdb.customers (id BIGINT PRIMARY KEY, name VARCHAR(255))");
        statement.execute("INSERT INTO appdb.customers (id, name) VALUES (1, 'one'), (2, 'two')");
      }
    }

    RecordingSink sink = new RecordingSink();
    MySqlSourceAdapter adapter =
        new MySqlSourceAdapter(
            new MySqlSourceAdapter.Dependencies() {
              private final io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader
                  chunkReader = new io.github.aandreakis.dblog.adapter.mysql.internal.JdbcMySqlChunkReader();

              @Override
              public Connection openSqlConnection(RelationalSourceConfig config) {
                try {
                  return DriverManager.getConnection(h2Jdbc);
                } catch (java.sql.SQLException failure) {
                  throw new io.github.aandreakis.dblog.DbLogRuntimeException(failure);
                }
              }

              @Override
              public List<io.github.aandreakis.dblog.core.schema.TableSchema> inspectSchemas(
                  Connection connection, RelationalSourceConfig config) {
                return List.of(
                    io.github.aandreakis.dblog.core.schema.TableSchema.create(
                        new io.github.aandreakis.dblog.core.model.TableId(
                            "sourceA", "appdb", "customers"),
                        List.of(
                            new io.github.aandreakis.dblog.core.schema.ColumnDefinition(
                                "id",
                                "bigint",
                                io.github.aandreakis.dblog.core.schema.NeutralColumnType.INTEGER,
                                true,
                                false),
                            new io.github.aandreakis.dblog.core.schema.ColumnDefinition(
                                "name",
                                "varchar(255)",
                                io.github.aandreakis.dblog.core.schema.NeutralColumnType.STRING,
                                false,
                                true)),
                        Instant.parse("2026-04-10T00:00:00Z")));
              }

              @Override
              public SourceChunkReader chunkReader() {
                return chunkReader;
              }

              @Override
              public WatermarkMetadataWriter watermarkWriter() {
                return new WatermarkMetadataWriter() {
                  @Override
                  public void ensureMetadataTable(Connection connection) {}

                  @Override
                  public void writeWatermark(
                      Connection sqlConnection,
                      String runId,
                      io.github.aandreakis.dblog.core.model.WatermarkToken token) {}
                };
              }

              @Override
              public HeartbeatMetadataWriter heartbeatWriter() {
                return new HeartbeatMetadataWriter() {
                  @Override
                  public void ensureHeartbeatTable(Connection connection) {}

                  @Override
                  public boolean writeHeartbeatIfDue(
                      Connection connection,
                      String runId,
                      String sourceStreamId,
                      Instant heartbeatTime,
                      Duration minimumInterval) {
                    return false;
                  }
                };
              }
            });

    RelationalSourceConfig config =
        new RelationalSourceConfig(
            "sourceA",
            "jdbc:mysql://127.0.0.1:3306/appdb",
            "dblog",
            "secret",
            "appdb",
            List.of("appdb.customers"),
            java.util.Map.of(),
            false);

    try (H2RuntimeStateStore stateStore =
            new H2RuntimeStateStore(tempDir.resolve("phase30-mysql-targeted-state"));
        DbLogApplication<?> app =
            DbLogApplication.open(adapter, config, stateStore, null, sink, 100)) {
      var server = app.controlPlaneServer("127.0.0.1", 0);
      server.start();
      try {
        String baseUrl = "http://127.0.0.1:" + server.boundPort();
        String submitResponse =
            postJson(
                baseUrl + "/api/v1/requests",
                """
                {"scope":"PRIMARY_KEYS","table":{"databaseName":"sourceA","schemaName":"appdb","tableName":"customers"},"primaryKeyLiterals":["1","9"]}
                """);
        String requestId = extractRequestId(submitResponse);

        app.processPendingRequests(Duration.ofMillis(5));

        String requestBody = get(baseUrl + "/api/v1/requests/" + requestId);
        assertThat(requestBody)
            .contains("\"scope\":\"PRIMARY_KEYS\"")
            .contains("\"state\":\"COMPLETED\"")
            .contains("\"missingPrimaryKeyLiterals\":[\"9\"]");
        assertThat(sink.emittedEvents).hasSize(1);
        assertThat(sink.emittedEvents)
            .extracting(ChangeEvent::captureOrigin)
            .containsOnly(CaptureOrigin.SELECT);
      } finally {
        server.stop();
      }
    }
  }

  private static String get(String url) throws Exception {
    HttpRequest request = HttpRequest.newBuilder(URI.create(url)).GET().build();
    return HttpClient.newHttpClient()
        .send(request, HttpResponse.BodyHandlers.ofString())
        .body();
  }

  private static String postJson(String url, String body) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    return HttpClient.newHttpClient()
        .send(request, HttpResponse.BodyHandlers.ofString())
        .body();
  }

  private static String extractRequestId(String jsonBody) {
    Matcher matcher = REQUEST_ID_PATTERN.matcher(jsonBody);
    if (!matcher.find()) {
      throw new IllegalArgumentException("requestId not found in response: " + jsonBody);
    }
    return matcher.group(1);
  }

  private static final class RecordingSink implements io.github.aandreakis.dblog.sink.api.ChangeEventSink {
    private final List<ChangeEvent> emittedEvents = new ArrayList<>();

    @Override
    public void appendEvents(List<ChangeEvent> events) {
      emittedEvents.addAll(events);
    }
  }
}
