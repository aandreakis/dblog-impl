package io.github.aandreakis.dblog.adapter.api;

/** Fail-closed runtime exception for source-protocol drift relative to the current contract. */
public class ProtocolDriftException extends IllegalStateException {
  public ProtocolDriftException(String message) {
    super(message);
  }

  public ProtocolDriftException(String message, Throwable cause) {
    super(message, cause);
  }
}
