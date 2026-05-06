package io.github.aandreakis.dblog.controlplane.service;

import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;

public interface RuntimeStatusProvider {
  RuntimeStatusSnapshot snapshot();

  static RuntimeStatusProvider idle() {
    return () -> new RuntimeStatusSnapshot("idle", "none", "UP", false, "No active runtime.");
  }

  record RuntimeStatusSnapshot(
      String mode,
      String adapter,
      String healthStatus,
      boolean requestSubmissionAvailable,
      String requestSubmissionMessage,
      String sourceId,
      String lastAcknowledgedCheckpoint,
      int pendingTransactions,
      int capturedTableCount,
      SourceFlowControlSnapshot sourceFlowControl) {
    public RuntimeStatusSnapshot(
        String mode,
        String adapter,
        String healthStatus,
        boolean requestSubmissionAvailable,
        String requestSubmissionMessage) {
      this(
          mode,
          adapter,
          healthStatus,
          requestSubmissionAvailable,
          requestSubmissionMessage,
          null,
          null,
          -1,
          -1,
          null);
    }

    public RuntimeStatusSnapshot(
        String mode,
        String adapter,
        String healthStatus,
        boolean requestSubmissionAvailable,
        String requestSubmissionMessage,
        String sourceId,
        String lastAcknowledgedCheckpoint,
        int pendingTransactions,
        int capturedTableCount) {
      this(
          mode,
          adapter,
          healthStatus,
          requestSubmissionAvailable,
          requestSubmissionMessage,
          sourceId,
          lastAcknowledgedCheckpoint,
          pendingTransactions,
          capturedTableCount,
          null);
    }
  }
}
