package io.github.aandreakis.dblog.verification.scenario;

/** Runtime exception for scenario-harness execution failures. */
public class ScenarioExecutionException extends IllegalStateException {
  public ScenarioExecutionException(String message) {
    super(message);
  }

  public ScenarioExecutionException(String message, Throwable cause) {
    super(messageWithCause(message, cause), cause);
  }

  private static String messageWithCause(String message, Throwable cause) {
    if (cause == null || cause.getMessage() == null || cause.getMessage().isBlank()) {
      return message;
    }
    return message + ": " + cause.getMessage();
  }
}
