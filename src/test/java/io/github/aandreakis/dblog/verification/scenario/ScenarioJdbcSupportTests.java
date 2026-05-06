package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ScenarioJdbcSupportTests {
  @TempDir Path tempDir;

  @Test
  void buildsStableH2SinkJdbcUrlFromPath() {
    String jdbcUrl = ScenarioJdbcSupport.sinkJdbcUrl(tempDir.resolve("sink-state"));

    assertThat(jdbcUrl).startsWith("jdbc:h2:file:");
    assertThat(jdbcUrl).contains("DB_CLOSE_ON_EXIT=FALSE");
  }

  @Test
  void loadsSinkSnapshotAndTrackedKeysFromNextScenarioStore() {
    try (JdbcScenarioStore store =
        new JdbcScenarioStore(
            "org.h2.Driver",
            ScenarioJdbcSupport.sinkJdbcUrl(tempDir.resolve("scenario-jdbc-support")))) {
      store.appendEvents(
          "scenario-1",
          "stage-1",
          List.of(
              event("1", "alpha"),
              event("2", "beta")));

      Map<String, String> snapshot = ScenarioJdbcSupport.loadSinkSnapshot(store, "scenario-1");
      Set<String> trackedKeys =
          ScenarioJdbcSupport.loadTrackedSinkKeys(
              store, "scenario-1", Set.of("app.public.widgets"));

      assertThat(snapshot).hasSize(2);
      assertThat(snapshot.keySet()).allMatch(key -> key.startsWith("app.public.widgets|"));
      assertThat(trackedKeys).hasSize(2);
    }
  }

  @Test
  void rendersScenarioPrimaryKeyLiteralForSingleAndCompositeKeys() {
    TableSchema single =
        TableSchema.create(
            new TableId("app", "public", "widgets"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));
    TableSchema composite =
        TableSchema.create(
            new TableId("app", "public", "pairs"),
            List.of(
                new ColumnDefinition("left_id", "bigint", NeutralColumnType.INTEGER, true, 1, false),
                new ColumnDefinition("right_id", "bigint", NeutralColumnType.INTEGER, true, 2, false),
                new ColumnDefinition("name", "varchar(255)", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));

    assertThat(
            ScenarioJdbcSupport.scenarioPrimaryKeyLiteral(
                single, Map.of("id", 7L, "name", "widget")))
        .isEqualTo("Long:7");
    assertThat(
            ScenarioJdbcSupport.scenarioPrimaryKeyLiteral(
                composite, Map.of("left_id", 1L, "right_id", 2L, "name", "pair")))
        .contains("left_id")
        .contains("right_id");
  }

  private static ChangeEvent event(String id, String name) {
    long numericId = Long.parseLong(id);
    return ChangeEventTestFixtures.fromRowMaps(
        new TableId("app", "public", "widgets"),
        OperationType.UPDATE,
        CaptureOrigin.LOG,
        Map.of("id", numericId),
        null,
        Map.of("id", numericId, "name", name),
        new OpaqueSourcePosition("pos:" + id),
        "tx-" + id,
        null);
  }
}
