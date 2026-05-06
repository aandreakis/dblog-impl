package io.github.aandreakis.dblog.integration.versionmatrix;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.core.model.TableId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Per-vendor scaffolding shared by {@link MySqlSourceVersionMatrixIT} and {@link
 * PostgresSourceVersionMatrixIT}. Holds the working directory, the typed sink's H2 file path that
 * is reused across every version of one source vendor, and the first version's schema fingerprint
 * so subsequent versions can prove the sink schema does not drift.
 *
 * <p>The harness sits under {@code build/test-output/<className>/} rather than {@code @TempDir}
 * so a failing run leaves the H2 sink, per-version state stores, and child-process logs on disk
 * for post-mortem inspection. The directory is wiped at construction time so a previous run's
 * artefacts cannot bleed into the next.
 */
final class VersionMatrixHarness {
  private final Path workDir;
  private final Path sinkPath;
  private Map<String, String> firstVersionSchemaSnapshot;

  private VersionMatrixHarness(Path workDir, Path sinkPath) {
    this.workDir = workDir;
    this.sinkPath = sinkPath;
  }

  static VersionMatrixHarness forVendor(Class<?> testClass, String sinkFileName) throws IOException {
    Path workDir = Path.of("build", "test-output", testClass.getSimpleName()).toAbsolutePath();
    deleteRecursively(workDir);
    Files.createDirectories(workDir);
    return new VersionMatrixHarness(workDir, workDir.resolve(sinkFileName));
  }

  Path sinkPath() {
    return sinkPath;
  }

  Path statePathFor(String versionLabel) {
    return workDir.resolve("state-" + versionLabel);
  }

  Path logFileFor(String versionLabel) {
    return workDir.resolve("log-" + versionLabel + ".log");
  }

  /**
   * Drives every dump shape (PRIMARY_KEYS, TABLE, ALL_TABLES) over the HTTP control plane, then
   * runs {@code streamingWriter} against the source and waits for the runtime to acknowledge the
   * resulting transaction past its prior checkpoint. Reaching the end implies the request
   * coordinator, dump-window coordinator, watermark reconciler, request pump, streaming pump, and
   * sink all worked end-to-end for this source version.
   */
  void runDumpAndStreamingSequence(
      FullProcessDbLogLauncher launcher,
      TableId mainTable,
      List<String> primaryKeyLiterals,
      ThrowingRunnable streamingWriter)
      throws Exception {
    String pkRequestId =
        launcher.submitRequestId(
            "PRIMARY_KEYS",
            mainTable.databaseName(),
            mainTable.schemaName(),
            mainTable.tableName(),
            primaryKeyLiterals);
    launcher.awaitRequestCompleted(pkRequestId, MatrixTimeouts.DUMP_REQUEST_COMPLETION);

    String tableRequestId =
        launcher.submitRequestId(
            "TABLE",
            mainTable.databaseName(),
            mainTable.schemaName(),
            mainTable.tableName(),
            List.of());
    launcher.awaitRequestCompleted(tableRequestId, MatrixTimeouts.DUMP_REQUEST_COMPLETION);

    String allTablesRequestId =
        launcher.submitRequestId("ALL_TABLES", null, null, null, List.of());
    launcher.awaitRequestCompleted(allTablesRequestId, MatrixTimeouts.DUMP_REQUEST_COMPLETION);

    String checkpointBeforeStreaming = launcher.currentCheckpoint();
    streamingWriter.run();
    launcher.awaitCheckpointAdvancePast(
        checkpointBeforeStreaming, MatrixTimeouts.STREAMING_CHECKPOINT_ADVANCE);
  }

  /**
   * Polls the source's {@code dblog_meta.heartbeats} singleton row until both {@code run_id} and
   * {@code last_beat_at} are populated. The streaming pump fires its first heartbeat on the very
   * first idle iteration of {@code runUntilStopped}, so this normally completes within a second
   * — but polling absorbs scheduling jitter on slow hosts.
   */
  void awaitHeartbeatPopulated(SqlConnectionSupplier connectionSupplier, Duration timeout)
      throws Exception {
    Instant deadline = Instant.now().plus(timeout);
    String lastObservation = "no row queried yet";
    // One connection + one PreparedStatement for the whole poll loop. The source DB is in a
    // testcontainer so a fresh connection per iteration is ~50-100 ms of TCP+auth that we
    // would burn for every empty observation.
    try (Connection connection = connectionSupplier.get();
        PreparedStatement statement =
            connection.prepareStatement(
                "SELECT run_id, last_beat_at FROM dblog_meta.heartbeats WHERE id = 1")) {
      while (Instant.now().isBefore(deadline)) {
        try (ResultSet rs = statement.executeQuery()) {
          if (rs.next()) {
            String runId = rs.getString("run_id");
            Timestamp lastBeat = rs.getTimestamp("last_beat_at");
            if (runId != null && !runId.isBlank() && lastBeat != null) {
              return;
            }
            lastObservation = "row exists but run_id=" + runId + " last_beat_at=" + lastBeat;
          } else {
            lastObservation = "singleton row id=1 missing";
          }
        }
        Thread.sleep(MatrixTimeouts.POLL_INTERVAL.toMillis());
      }
    }
    throw new AssertionError(
        "dblog_meta.heartbeats was not populated by the streaming pump within "
            + timeout
            + " (last observation: "
            + lastObservation
            + ")");
  }

  /**
   * On the first call, captures the typed sink's mirror schema as the baseline. On every
   * subsequent call, asserts the current schema is byte-identical — proving the sink built from
   * the first source version remains usable, with the same DDL, by every other version.
   */
  void assertSinkSchemaStableAcrossVersions() throws Exception {
    Map<String, String> currentFingerprint = TypedSinkReader.readSchemaFingerprint(sinkPath);
    assertThat(currentFingerprint).as("sink mirror schema must not be empty").isNotEmpty();
    if (firstVersionSchemaSnapshot == null) {
      firstVersionSchemaSnapshot = currentFingerprint;
      return;
    }
    assertThat(currentFingerprint)
        .as("typed sink mirror schema must remain identical across source versions")
        .isEqualTo(firstVersionSchemaSnapshot);
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (var stream = Files.walk(root)) {
      stream
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException ignored) {
                  // Best-effort cleanup; if a file is still locked we'll discover the
                  // contamination at the next assertion.
                }
              });
    }
  }

  @FunctionalInterface
  interface ThrowingRunnable {
    void run() throws Exception;
  }

  @FunctionalInterface
  interface SqlConnectionSupplier {
    Connection get() throws SQLException;
  }
}
