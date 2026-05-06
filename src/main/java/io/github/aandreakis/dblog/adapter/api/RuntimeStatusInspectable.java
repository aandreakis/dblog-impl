package io.github.aandreakis.dblog.adapter.api;

import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;

/**
 * Operator-facing runtime status facets that adapter-owned runtimes may expose.
 *
 * <p>Methods returning {@code int} use {@code -1} as a sentinel for "unknown or not applicable
 * for this adapter," not as a real count. Implementations either return a real count or leave
 * the default {@code -1} in place to signal the absence of a meaningful value (for example,
 * an adapter with no intermediate pending-transaction queue will leave
 * {@link #pendingTransactionCount()} at {@code -1}).
 */
public interface RuntimeStatusInspectable {
  default String lastAcknowledgedCheckpointDisplayValue() {
    return null;
  }

  /**
   * Count of committed source transactions staged for the orchestrator but not yet drained, or
   * {@code -1} if the adapter has no intermediate queue and the notion is not applicable.
   */
  default int pendingTransactionCount() {
    return -1;
  }

  /** Number of tables the runtime is currently configured to capture, or {@code -1} if unknown. */
  default int capturedTableCount() {
    return -1;
  }

  default SourceFlowControlSnapshot sourceFlowControlSnapshot() {
    return SourceFlowControlSnapshot.unavailable();
  }
}
