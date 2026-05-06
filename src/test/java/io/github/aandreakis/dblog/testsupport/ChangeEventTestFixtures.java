package io.github.aandreakis.dblog.testsupport;

import io.github.aandreakis.dblog.core.model.CaptureOrigin;
import io.github.aandreakis.dblog.core.model.ChangeEvent;
import io.github.aandreakis.dblog.core.model.ImmutableRowImage;
import io.github.aandreakis.dblog.core.model.OperationType;
import io.github.aandreakis.dblog.core.model.SourcePosition;
import io.github.aandreakis.dblog.core.model.TableId;
import java.util.Map;

/**
 * Test-only fixture builders for {@link ChangeEvent}. Production code constructs events via
 * the canonical record constructor with pre-built {@link ImmutableRowImage} values (typically
 * via {@link ImmutableRowImage#ofLayout} on the adapter hot path). Tests and fixture helpers
 * that assemble events from ad-hoc {@code Map<String, Object>} literals should use
 * {@link #fromRowMaps} instead so the {@code Map}-to-{@code ImmutableRowImage} wrapping stays
 * out of every call site.
 *
 * <p>Intentionally lives in {@code testsupport} alongside
 * {@link MySqlTestUserGrants} so it is visible from {@code test}, {@code integrationTest}, and
 * {@code e2eTest} source sets without polluting the main production API surface.
 */
public final class ChangeEventTestFixtures {

  private ChangeEventTestFixtures() {}

  /**
   * Builds a {@link ChangeEvent} from {@code Map<String, Object>} row shapes, wrapping each
   * row as an {@link ImmutableRowImage}. {@code beforeRow} and {@code afterRow} may be
   * {@code null}; {@code primaryKey} must be non-null and non-empty.
   */
  public static ChangeEvent fromRowMaps(
      TableId tableId,
      OperationType operationType,
      CaptureOrigin captureOrigin,
      Map<String, Object> primaryKey,
      Map<String, Object> beforeRow,
      Map<String, Object> afterRow,
      SourcePosition sourcePosition,
      String transactionId,
      String dumpId) {
    return new ChangeEvent(
        tableId,
        operationType,
        captureOrigin,
        ImmutableRowImage.of(primaryKey),
        beforeRow == null ? null : ImmutableRowImage.of(beforeRow),
        afterRow == null ? null : ImmutableRowImage.of(afterRow),
        sourcePosition,
        transactionId,
        dumpId);
  }
}
