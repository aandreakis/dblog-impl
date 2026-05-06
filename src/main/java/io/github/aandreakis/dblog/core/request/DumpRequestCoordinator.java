package io.github.aandreakis.dblog.core.request;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import java.util.Optional;

public interface DumpRequestCoordinator<TX extends SourceTransaction<?>> {
  Optional<ScheduledRequestBatch<TX>> coordinateNextBatch();

  void acknowledgeCompletedBatch(ScheduledRequestBatch<TX> batch);

  default int pendingRequestCount() {
    return -1;
  }
}
