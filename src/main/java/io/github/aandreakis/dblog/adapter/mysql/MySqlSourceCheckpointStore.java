package io.github.aandreakis.dblog.adapter.mysql;

import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.schema.FullDumpRequiredSignal;
import io.github.aandreakis.dblog.core.schema.SchemaUncertaintySignal;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** Typed wrapper around the generic next.state checkpoint contract for MySQL positions. */
public final class MySqlSourceCheckpointStore {
  private final RuntimeStateStore stateStore;
  private final MySqlSourceCheckpointCodec codec = new MySqlSourceCheckpointCodec();

  public MySqlSourceCheckpointStore(RuntimeStateStore stateStore) {
    this.stateStore = Objects.requireNonNull(stateStore, "stateStore");
  }

  public void save(String sourceId, MySqlSourcePosition checkpoint) {
    stateStore
        .streamPositions()
        .saveCheckpoint(requireNonBlank(sourceId, "sourceId"), Objects.requireNonNull(checkpoint, "checkpoint"));
  }

  public Optional<MySqlSourcePosition> load(String sourceId) {
    return stateStore
        .streamPositions()
        .loadCheckpoint(requireNonBlank(sourceId, "sourceId"))
        .map(this::toMySqlPosition);
  }

  public void saveBootstrapResumePosition(String sourceId, MySqlSourcePosition position) {
    stateStore
        .streamPositions()
        .saveBootstrapPosition(
            requireNonBlank(sourceId, "sourceId"), Objects.requireNonNull(position, "position"));
  }

  public Optional<MySqlSourcePosition> loadBootstrapResumePosition(String sourceId) {
    return stateStore
        .streamPositions()
        .loadBootstrapPosition(requireNonBlank(sourceId, "sourceId"))
        .map(this::toMySqlPosition);
  }

  public void clearBootstrapResumePosition(String sourceId) {
    stateStore.streamPositions().clearBootstrapPosition(requireNonBlank(sourceId, "sourceId"));
  }

  public void saveFullDumpRequiredSignal(String sourceId, TableId tableId, String reason) {
    stateStore.schemas().saveFullDumpRequiredSignal(
        new FullDumpRequiredSignal(
            requireNonBlank(sourceId, "sourceId"),
            tableId,
            requireNonBlank(reason, "reason"),
            Instant.now()));
  }

  public void saveSchemaUncertaintySignal(String sourceId, TableId tableId, String reason) {
    Instant detectedAt = Instant.now();
    stateStore.schemas().saveSchemaUncertaintySignal(
        new SchemaUncertaintySignal(
            requireNonBlank(sourceId, "sourceId"),
            tableId,
            requireNonBlank(reason, "reason"),
            detectedAt,
            detectedAt,
            1));
  }

  public void saveObservedTableSchema(TableSchema tableSchema) {
    stateStore.schemas().saveObservedSchema(Objects.requireNonNull(tableSchema, "tableSchema"));
  }

  public void clearSchemaUncertainty(String sourceId, TableId tableId) {
    stateStore
        .schemas()
        .clearSchemaUncertaintySignal(
            requireNonBlank(sourceId, "sourceId"), tableId == null ? "" : tableId.displayName());
  }

  public void invalidateRuntimeStateForPkChange(String sourceId, String reason) {
    stateStore.invalidateRuntimeStateForPkChange(
        requireNonBlank(sourceId, "sourceId"), requireNonBlank(reason, "reason"));
  }

  private MySqlSourcePosition toMySqlPosition(SourcePosition position) {
    Objects.requireNonNull(position, "position");
    if (position instanceof MySqlSourcePosition mySqlSourcePosition) {
      return mySqlSourcePosition;
    }
    return codec.decode(position.displayValue());
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
