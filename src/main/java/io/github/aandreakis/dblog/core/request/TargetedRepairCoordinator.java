package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.schema.TableSchema;

public interface TargetedRepairCoordinator<TX extends SourceTransaction<?>> {
  TargetedRepairResult<TX> coordinate(DumpRequest request, TableSchema schema);

  void acknowledge(TargetedRepairOutcome<TX> outcome);
}
