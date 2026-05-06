package io.github.aandreakis.dblog.adapter.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.core.model.OpaqueSourcePosition;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.h2.H2RuntimeStateStore;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PostgresSourceCheckpointStoreTests {
  @TempDir Path tempDir;

  @Test
  void persistsTypedPostgresCheckpointAcrossRestartBoundary() {
    H2RuntimeStateStore stateStore =
        new H2RuntimeStateStore(tempDir.resolve("postgres-checkpoint-store"));
    PostgresSourceCheckpointStore checkpointStore = new PostgresSourceCheckpointStore(stateStore);

    checkpointStore.save("postgres-source", PostgresLsn.parse("0/16db011"));

    H2RuntimeStateStore reloaded =
        new H2RuntimeStateStore(tempDir.resolve("postgres-checkpoint-store"));
    PostgresSourceCheckpointStore reloadedCheckpointStore =
        new PostgresSourceCheckpointStore(reloaded);

    assertThat(reloadedCheckpointStore.load("postgres-source"))
        .contains(PostgresLsn.parse("0/16DB011"));
    assertThat(reloaded.streamPositions().loadCheckpoint("postgres-source").orElseThrow().displayValue())
        .isEqualTo("0/16DB011");
  }

  @Test
  void failsClosedWhenPersistedCheckpointIsNotAPostgresLsn() {
    RuntimeStateStore stateStore =
        new StubRuntimeStateStore(Optional.of(new OpaqueSourcePosition("not-an-lsn")));

    assertThatThrownBy(() -> new PostgresSourceCheckpointStore(stateStore).load("postgres-source"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("PostgreSQL LSN");
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
