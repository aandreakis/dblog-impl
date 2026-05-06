package io.github.aandreakis.dblog.runtime.host;

import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.runtime.observer.RuntimeLoopObserver;
import io.github.aandreakis.dblog.tap.NoopTap;
import io.github.aandreakis.dblog.tap.Tap;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DbLogRuntimeHostLifecycle<TX extends SourceTransaction<?>> implements AutoCloseable {
  private final RuntimeSession<TX> session;
  private final CheckpointFlushPolicy checkpointFlushPolicy;
  private final RuntimeLoopObserver<TX> observer;
  private final Tap tap;
  private final AtomicBoolean stopRequested = new AtomicBoolean(false);

  public DbLogRuntimeHostLifecycle(RuntimeSession<TX> session) {
    this(session, CheckpointFlushPolicy.defaults(), RuntimeLoopObserver.noop(), NoopTap.INSTANCE);
  }

  public DbLogRuntimeHostLifecycle(
      RuntimeSession<TX> session,
      CheckpointFlushPolicy checkpointFlushPolicy,
      RuntimeLoopObserver<TX> observer) {
    this(session, checkpointFlushPolicy, observer, NoopTap.INSTANCE);
  }

  public DbLogRuntimeHostLifecycle(
      RuntimeSession<TX> session,
      CheckpointFlushPolicy checkpointFlushPolicy,
      RuntimeLoopObserver<TX> observer,
      Tap tap) {
    this.session = Objects.requireNonNull(session, "session");
    this.checkpointFlushPolicy =
        Objects.requireNonNull(checkpointFlushPolicy, "checkpointFlushPolicy");
    this.observer = Objects.requireNonNull(observer, "observer");
    this.tap = Objects.requireNonNull(tap, "tap");
  }

  public RuntimeSession<TX> session() {
    return session;
  }

  public int runStreamingDrain(String stageLabel, Duration idleTimeout) throws java.sql.SQLException {
    if (stopRequested.get()) {
      return 0;
    }
    return session.streamingPump(checkpointFlushPolicy, observer, tap).drainStreaming(stageLabel, idleTimeout);
  }

  public void requestStop() {
    stopRequested.set(true);
  }

  @Override
  public void close() throws Exception {
    session.close();
  }
}
