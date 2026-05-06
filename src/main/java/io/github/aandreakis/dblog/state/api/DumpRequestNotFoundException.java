package io.github.aandreakis.dblog.state.api;

/**
 * Thrown when a control-plane request lookup names a request that does not exist in the durable
 * store. Mapped to HTTP 404 at the control-plane boundary so operators can distinguish "no such
 * request" from "bad request payload" (400).
 */
public final class DumpRequestNotFoundException extends RuntimeException {
  public DumpRequestNotFoundException(String requestId) {
    super("unknown dump requestId: " + requestId);
  }
}
