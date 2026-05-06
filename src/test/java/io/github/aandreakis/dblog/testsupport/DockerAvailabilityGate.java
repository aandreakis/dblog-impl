package io.github.aandreakis.dblog.testsupport;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.testcontainers.DockerClientFactory;

/**
 * Single source of truth for the "skip this integration test when Docker is unreachable" gate.
 * Sits in {@code testsupport/} alongside {@link MySqlTestUserGrants} so both the
 * {@code integrationTest} and {@code e2eTest} source sets can use it.
 *
 * <p>Includes the underlying exception type in the assume message so an operator debugging a
 * misbehaving Docker daemon (socket permissions, missing class, etc.) can pin the cause from the
 * skipped-test report without having to re-run with logging. JUnit's skipped-test report already
 * shows the test class name, so the message intentionally does NOT carry a per-test label —
 * locality is provided by JUnit's own report context.
 */
public final class DockerAvailabilityGate {
  private DockerAvailabilityGate() {}

  /**
   * Skips the calling test (via JUnit's assume) when {@link DockerClientFactory#isDockerAvailable}
   * returns false or throws. Catches {@link Throwable} so a missing-class or initialization-error
   * surface from Testcontainers does not fail the test — Docker simply isn't available, so we
   * skip rather than fail.
   */
  public static void assumeDockerIsAvailable() {
    try {
      assumeTrue(
          DockerClientFactory.instance().isDockerAvailable(),
          "Docker is unavailable; skipping integration test.");
    } catch (Throwable ex) {
      assumeTrue(
          false,
          "Docker is unavailable; skipping integration test. Cause: "
              + ex.getClass().getSimpleName()
              + ": "
              + ex.getMessage());
    }
  }
}
