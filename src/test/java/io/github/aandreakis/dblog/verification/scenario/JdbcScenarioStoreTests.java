package io.github.aandreakis.dblog.verification.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.testsupport.ChangeEventTestFixtures;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JdbcScenarioStoreTests {
  @TempDir Path tempDir;

  @Test
  void updatesCurrentRowOncePerKeyWithinOneAppendBatch() {
    try (JdbcScenarioStore store =
        new JdbcScenarioStore(
            "org.h2.Driver",
            "jdbc:h2:file:"
                + tempDir.resolve("scenario-store").toAbsolutePath().normalize()
                + ";DB_CLOSE_ON_EXIT=FALSE")) {
      store.appendEvents(
          "scenario-1",
          "stage-1",
          List.of(
              event("1", "widget-1"),
              event("1", "widget-1-updated"),
              event("2", "widget-2")));

      assertThat(store.loadEvents("scenario-1")).hasSize(3);
      List<ScenarioRowState> currentRows = store.loadCurrentRows("scenario-1");
      assertThat(currentRows).hasSize(2);
      assertThat(currentRows)
          .filteredOn(row -> row.primaryKeyLiteral().contains("1"))
          .singleElement()
          .extracting(ScenarioRowState::payload)
          .asString()
          .contains("widget-1-updated");
    }
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
