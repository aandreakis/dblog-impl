package io.github.aandreakis.dblog.adapter.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MySqlSourceCheckpointStoreTests {
  @TempDir Path tempDir;

  @Test
  void persistsTypedMySqlCheckpointAcrossRestartBoundary() {
    H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("mysql-checkpoint-store"));
    MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);

    checkpointStore.save(
        "mysql-source",
        new MySqlSourcePosition(
            "mysql-bin.000123",
            456789L,
            "24BC785E-9D1B-11EE-B9D1-0242AC120002:1-42"));

    H2RuntimeStateStore reloaded = new H2RuntimeStateStore(tempDir.resolve("mysql-checkpoint-store"));
    MySqlSourceCheckpointStore reloadedCheckpointStore = new MySqlSourceCheckpointStore(reloaded);

    assertThat(reloadedCheckpointStore.load("mysql-source"))
        .contains(
            new MySqlSourcePosition(
                "mysql-bin.000123",
                456789L,
                "24BC785E-9D1B-11EE-B9D1-0242AC120002:1-42"));
    assertThat(reloaded.streamPositions().loadCheckpoint("mysql-source").orElseThrow().displayValue())
        .isEqualTo(
            "mysql-bin.000123:456789;gtid=24BC785E-9D1B-11EE-B9D1-0242AC120002:1-42");
  }

  @Test
  void failsClosedWhenPersistedCheckpointIsNotAMySqlPosition() {
    RuntimeStateStore stateStore = new StubRuntimeStateStore(Optional.of(new OpaqueSourcePosition("not-a-mysql-position")));

    assertThatThrownBy(() -> new MySqlSourceCheckpointStore(stateStore).load("mysql-source"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("<binlogFilename>:<binlogPosition>");
  }

  @Test
  void persistsBootstrapResumePositionAndClearsItWhenCheckpointAdvances() {
    H2RuntimeStateStore stateStore = new H2RuntimeStateStore(tempDir.resolve("mysql-bootstrap-store"));
    MySqlSourceCheckpointStore checkpointStore = new MySqlSourceCheckpointStore(stateStore);

    MySqlSourcePosition bootstrap =
        new MySqlSourcePosition("mysql-bin.000321", 654321L, "uuid:1-9");
    MySqlSourcePosition checkpoint =
        new MySqlSourcePosition("mysql-bin.000321", 654999L, "uuid:1-10");

    checkpointStore.saveBootstrapResumePosition("mysql-source", bootstrap);

    assertThat(checkpointStore.loadBootstrapResumePosition("mysql-source")).contains(bootstrap);

    checkpointStore.save("mysql-source", checkpoint);

    assertThat(checkpointStore.load("mysql-source")).contains(checkpoint);
    assertThat(checkpointStore.loadBootstrapResumePosition("mysql-source")).isEmpty();
  }

  private static final class StubRuntimeStateStore implements RuntimeStateStore {
    private final Optional<SourcePosition> checkpoint;

    private StubRuntimeStateStore(Optional<SourcePosition> checkpoint) {
      this.checkpoint = checkpoint;
    }

    @Override
    public io.github.aandreakis.dblog.state.api.StreamPositionRepository streamPositions() {
      return new io.github.aandreakis.dblog.state.api.StreamPositionRepository() {
        @Override
        public void saveCheckpoint(String sourceId, SourcePosition position) {}

        @Override
        public Optional<SourcePosition> loadCheckpoint(String sourceId) {
          return checkpoint;
        }

        @Override
        public void saveBootstrapPosition(String sourceId, SourcePosition position) {}

        @Override
        public Optional<SourcePosition> loadBootstrapPosition(String sourceId) {
          return Optional.empty();
        }

        @Override
        public void clearBootstrapPosition(String sourceId) {}
      };
    }

    @Override
    public io.github.aandreakis.dblog.state.api.DumpRequestRepository dumpRequests() {
      throw new UnsupportedOperationException();
    }

    @Override
    public io.github.aandreakis.dblog.state.api.DumpProgressRepository dumpProgress() {
      throw new UnsupportedOperationException();
    }

    @Override
    public io.github.aandreakis.dblog.state.api.SchemaStateRepository schemas() {
      throw new UnsupportedOperationException();
    }

    @Override
    public io.github.aandreakis.dblog.state.api.SourceOwnershipRepository ownership() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void invalidateRuntimeStateForPkChange(String sourceId, String reason) {
      throw new UnsupportedOperationException();
    }
  }
}
