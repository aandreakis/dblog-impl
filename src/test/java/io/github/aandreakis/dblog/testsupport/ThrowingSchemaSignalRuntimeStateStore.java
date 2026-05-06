package io.github.aandreakis.dblog.testsupport;

import io.github.aandreakis.dblog.core.schema.FullDumpRequiredSignal;
import io.github.aandreakis.dblog.core.schema.SchemaUncertaintySignal;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import io.github.aandreakis.dblog.state.api.DumpProgressRepository;
import io.github.aandreakis.dblog.state.api.DumpRequestRepository;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.api.SchemaStateRepository;
import io.github.aandreakis.dblog.state.api.SourceOwnershipRepository;
import io.github.aandreakis.dblog.state.api.StreamPositionRepository;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class ThrowingSchemaSignalRuntimeStateStore implements RuntimeStateStore {
  private final RuntimeStateStore delegate;
  private final RuntimeException fullDumpRequiredFailure;
  private final RuntimeException schemaUncertaintyFailure;

  private ThrowingSchemaSignalRuntimeStateStore(
      RuntimeStateStore delegate,
      RuntimeException fullDumpRequiredFailure,
      RuntimeException schemaUncertaintyFailure) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.fullDumpRequiredFailure = fullDumpRequiredFailure;
    this.schemaUncertaintyFailure = schemaUncertaintyFailure;
  }

  public static ThrowingSchemaSignalRuntimeStateStore failFullDumpRequiredSignal(
      RuntimeStateStore delegate, RuntimeException failure) {
    return new ThrowingSchemaSignalRuntimeStateStore(
        delegate, Objects.requireNonNull(failure, "failure"), null);
  }

  public static ThrowingSchemaSignalRuntimeStateStore failSchemaUncertaintySignal(
      RuntimeStateStore delegate, RuntimeException failure) {
    return new ThrowingSchemaSignalRuntimeStateStore(
        delegate, null, Objects.requireNonNull(failure, "failure"));
  }

  @Override
  public StreamPositionRepository streamPositions() {
    return delegate.streamPositions();
  }

  @Override
  public DumpRequestRepository dumpRequests() {
    return delegate.dumpRequests();
  }

  @Override
  public DumpProgressRepository dumpProgress() {
    return delegate.dumpProgress();
  }

  @Override
  public SchemaStateRepository schemas() {
    SchemaStateRepository schemas = delegate.schemas();
    return new SchemaStateRepository() {
      @Override
      public void saveContractSchema(TableSchema schema) {
        schemas.saveContractSchema(schema);
      }

      @Override
      public Optional<TableSchema> loadContractSchema(String tableDisplayName) {
        return schemas.loadContractSchema(tableDisplayName);
      }

      @Override
      public List<TableSchema> loadAllContractSchemas() {
        return schemas.loadAllContractSchemas();
      }

      @Override
      public void saveObservedSchema(TableSchema schema) {
        schemas.saveObservedSchema(schema);
      }

      @Override
      public Optional<TableSchema> loadObservedSchema(String tableDisplayName) {
        return schemas.loadObservedSchema(tableDisplayName);
      }

      @Override
      public List<TableSchema> loadAllObservedSchemas() {
        return schemas.loadAllObservedSchemas();
      }

      @Override
      public void saveFullDumpRequiredSignal(FullDumpRequiredSignal signal) {
        if (fullDumpRequiredFailure != null) {
          throw fullDumpRequiredFailure;
        }
        schemas.saveFullDumpRequiredSignal(signal);
      }

      @Override
      public List<FullDumpRequiredSignal> loadFullDumpRequiredSignals() {
        return schemas.loadFullDumpRequiredSignals();
      }

      @Override
      public void saveSchemaUncertaintySignal(SchemaUncertaintySignal signal) {
        if (schemaUncertaintyFailure != null) {
          throw schemaUncertaintyFailure;
        }
        schemas.saveSchemaUncertaintySignal(signal);
      }

      @Override
      public void clearSchemaUncertaintySignal(String sourceId, String tableDisplayName) {
        schemas.clearSchemaUncertaintySignal(sourceId, tableDisplayName);
      }

      @Override
      public void deleteSchemaUncertaintySignals(String sourceId) {
        schemas.deleteSchemaUncertaintySignals(sourceId);
      }

      @Override
      public List<SchemaUncertaintySignal> loadSchemaUncertaintySignals() {
        return schemas.loadSchemaUncertaintySignals();
      }
    };
  }

  @Override
  public SourceOwnershipRepository ownership() {
    return delegate.ownership();
  }

  @Override
  public void invalidateRuntimeStateForPkChange(String sourceId, String reason) {
    delegate.invalidateRuntimeStateForPkChange(sourceId, reason);
  }
}
