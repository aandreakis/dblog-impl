package io.github.aandreakis.dblog.runtime.host;

/** Whether a runtime failure should be retried or treated as a hard contract breach. */
public enum RuntimeFailureDisposition {
  RETRYABLE_AVAILABILITY,
  FAIL_HARD_CONTRACT_BREACH
}
