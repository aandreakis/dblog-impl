package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import java.time.Instant;
import java.util.List;

public interface SourceTransaction<P extends SourcePosition> {
  String transactionId();

  P checkpointPosition();

  Instant commitTimestamp();

  List<ChangeEvent> events();

  default int eventCount() {
    return events().size();
  }
}
