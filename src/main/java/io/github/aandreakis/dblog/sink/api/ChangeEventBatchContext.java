package io.github.aandreakis.dblog.sink.api;

import java.time.Instant;

/**
 * Per-batch metadata passed to a {@link ContextualChangeEventSink}. {@code stageLabel} identifies
 * the originating runtime stage; {@code sourceCommitTimestamp} is the commit timestamp of the
 * source transaction that carried the batch's checkpoint, or {@code null} when no source-side
 * timestamp is available.
 */
public record ChangeEventBatchContext(String stageLabel, Instant sourceCommitTimestamp) {}
