package io.github.aandreakis.dblog.adapter.api;

import java.util.Objects;

public record OpenedSourceRuntime<TX extends SourceTransaction<?>>(
    SourceRuntime<TX> runtime,
    SourceChunkReader chunkReader,
    String loadedCheckpointDisplayValue) {
  public OpenedSourceRuntime {
    Objects.requireNonNull(runtime, "runtime");
    Objects.requireNonNull(chunkReader, "chunkReader");
  }
}
