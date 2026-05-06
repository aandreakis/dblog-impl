package io.github.aandreakis.dblog.runtime.bootstrap;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.request.DumpRequestCoordinator;
import io.github.aandreakis.dblog.core.request.DumpWindowCoordinator;
import io.github.aandreakis.dblog.core.request.TargetedRepairCoordinator;
import io.github.aandreakis.dblog.runtime.host.RuntimeSession;
import io.github.aandreakis.dblog.runtime.loop.RuntimeRequestPump;
import io.github.aandreakis.dblog.runtime.loop.RuntimeStreamingPump;
import java.time.Duration;
import java.util.Objects;

public record RelationalRuntimeStack<TX extends SourceTransaction<?>>(
    RuntimeSession<TX> session,
    RuntimeStreamingPump<TX> streamingPump,
    DumpWindowCoordinator<TX> dumpWindowCoordinator,
    TargetedRepairCoordinator<TX> targetedRepairCoordinator,
    DumpRequestCoordinator<TX> coordinator,
    RuntimeRequestPump<TX> requestPump) {
  public RelationalRuntimeStack {
    Objects.requireNonNull(session, "session");
    Objects.requireNonNull(streamingPump, "streamingPump");
    Objects.requireNonNull(dumpWindowCoordinator, "dumpWindowCoordinator");
    Objects.requireNonNull(targetedRepairCoordinator, "targetedRepairCoordinator");
    Objects.requireNonNull(coordinator, "coordinator");
    Objects.requireNonNull(requestPump, "requestPump");
  }

  public void processPendingRequests(Duration idleDrainTimeout) throws Exception {
    requestPump.processPendingRequests(coordinator, idleDrainTimeout);
  }

  public int drainStreaming(String stageLabel, Duration idleTimeout) throws Exception {
    return streamingPump.drainStreaming(Objects.requireNonNull(stageLabel, "stageLabel"), idleTimeout);
  }
}
