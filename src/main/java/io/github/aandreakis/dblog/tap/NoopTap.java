package io.github.aandreakis.dblog.tap;

import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.request.DumpRequestState;
import io.github.aandreakis.dblog.core.request.DumpScope;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.List;
import java.util.Map;

/**
 * Stub {@link Tap}. Every method is an empty body so the JIT can inline each {@code tap.onX(...)}
 * call site down to nothing when the tap is off.
 *
 * <p>Supplied by the Spring {@code Tap} bean when {@code dblog.tap.enabled=false}, and passed
 * directly in tests / scenario code paths that don't care about observability. When the tap is
 * on, the bean supplies {@link ActiveTap} instead — the toggle is resolved once at boot via
 * type dispatch, not per-method.
 */
public final class NoopTap implements Tap {
  public static final NoopTap INSTANCE = new NoopTap();

  private NoopTap() {}

  @Override
  public void onSinkBatchStart() {}

  @Override
  public void onSinkBatchCommit() {}

  @Override
  public void onSinkEvent(ChangeEvent event, String sinkName) {}

  @Override
  public void onCdcBatch(List<ChangeEvent> events) {}

  @Override
  public void onWatermarkWritten(WatermarkLevel level, WatermarkToken token) {}

  @Override
  public void onWatermarkReceived(WatermarkLevel level, WatermarkToken token, SourcePosition lsn) {}

  @Override
  public void onChunkSelected(String requestId, Chunk chunk) {}

  @Override
  public void onChunkCollision(
      TableSchema schema, ImmutableRowImage removedRow, ChangeEvent cause) {}

  @Override
  public void onChunkCompleted(
      String requestId, Chunk chunk, List<ChangeEvent> emittedEvents, SourcePosition hwLsn) {}

  @Override
  public void onCheckpointAdvanced(SourcePosition position, int bufferedEvents, String reason) {}

  @Override
  public void onRequestTransition(
      String requestId,
      DumpScope scope,
      TableId table,
      DumpRequestState previous,
      DumpRequestState current,
      String reason) {}

  @Override
  public void onError(String exceptionClass, String message, Map<String, Object> context) {}
}
