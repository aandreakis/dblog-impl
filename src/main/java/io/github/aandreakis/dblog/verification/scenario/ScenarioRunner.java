package io.github.aandreakis.dblog.verification.scenario;

/** Small production scenario-runner contract for real-pipeline stress runs. */
public interface ScenarioRunner {
  void run() throws Exception;
}
