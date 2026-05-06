package io.github.aandreakis.dblog.runtime.host;

import io.github.aandreakis.dblog.adapter.api.SourceChunkReader;
import io.github.aandreakis.dblog.adapter.api.SourceRuntime;
import io.github.aandreakis.dblog.adapter.api.SourceTransaction;
import io.github.aandreakis.dblog.core.checkpoint.CheckpointFlushPolicy;
import io.github.aandreakis.dblog.runtime.loop.RuntimeStreamingPump;
import io.github.aandreakis.dblog.runtime.observer.RuntimeLoopObserver;
import io.github.aandreakis.dblog.sink.api.ChangeEventSink;
import io.github.aandreakis.dblog.tap.Tap;
import java.util.Objects;

public final class RuntimeSession<TX extends SourceTransaction<?>> implements AutoCloseable {
  private final SourceRuntime<TX> runtime;
  private final SourceChunkReader chunkReader;
  private final ChangeEventSink sink;

  public RuntimeSession(SourceRuntime<TX> runtime, SourceChunkReader chunkReader, ChangeEventSink sink) {
    this.runtime = Objects.requireNonNull(runtime, "runtime");
    this.chunkReader = Objects.requireNonNull(chunkReader, "chunkReader");
    this.sink = Objects.requireNonNull(sink, "sink");
  }

  public SourceRuntime<TX> runtime() {
    return runtime;
  }

  public SourceChunkReader chunkReader() {
    return chunkReader;
  }

  public ChangeEventSink sink() {
    return sink;
  }

  public RuntimeStreamingPump<TX> streamingPump(
      CheckpointFlushPolicy checkpointFlushPolicy, RuntimeLoopObserver<TX> observer, Tap tap) {
    return new RuntimeStreamingPump<>(
        runtime,
        sink,
        checkpointFlushPolicy,
        observer == null ? RuntimeLoopObserver.noop() : observer,
        tap);
  }

  @Override
  public void close() throws Exception {
    Exception firstFailure = null;
    try {
      sink.requestStop();
    } catch (RuntimeException failure) {
      firstFailure = failure;
    }
    try {
      sink.close();
    } catch (Exception failure) {
      if (firstFailure != null) {
        firstFailure.addSuppressed(failure);
      } else {
        firstFailure = failure;
      }
    }
    try {
      runtime.close();
    } catch (Exception failure) {
      if (firstFailure != null) {
        firstFailure.addSuppressed(failure);
      } else {
        firstFailure = failure;
      }
    }
    if (firstFailure != null) {
      throw firstFailure;
    }
  }
}
