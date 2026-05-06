package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.Optional;

public interface DumpWindowCoordinator<TX extends SourceTransaction<?>> {
  Optional<DumpWindowOutcome<TX>> coordinateNextTableChunk(
      String jobId, TableSchema schema, int chunkSize);

  boolean hasRemainingTableWork(String jobId, TableSchema schema, int chunkSize);

  void acknowledgeCompletedBatch(DumpWindowOutcome<TX> outcome);
}
