package io.github.aandreakis.dblog.controlplane.service;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import java.util.List;

public interface RequestSubmissionGateway {
  DumpRequest submit(DumpScope scope, TableId tableId, List<PrimaryKeyTuple> primaryKeyTuples);
}
