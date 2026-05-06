package io.github.aandreakis.dblog.config;

/** Boot mode selected via {@code dblog.boot-mode}; defaults to {@link #RUNTIME} when unset. */
public enum DbLogBootstrapMode {
  /** Long-lived CDC pump. */
  RUNTIME,
  /** Runs a verification scenario and exits. */
  SCENARIO,
  /** Validates source-adapter readiness (config, schemas, preflight, bootstrap open) and exits without running the pump. */
  STARTUP_CHECK
}
