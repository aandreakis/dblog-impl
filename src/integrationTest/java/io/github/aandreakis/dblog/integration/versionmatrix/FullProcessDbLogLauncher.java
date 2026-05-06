package io.github.aandreakis.dblog.integration.versionmatrix;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Launches the full DBLog Spring Boot application as a child JVM via {@code java -jar
 * dblog-impl-<version>.jar --...}. The child is the same artifact production operators
 * deploy; there are no test-only seams or in-process tweaks.
 *
 * <p>Shutdown is by {@link Process#destroy()} (POSIX SIGTERM), which triggers DBLog's own JVM
 * shutdown hook on the child's hook thread — exactly the production graceful-shutdown path.
 * The runtime's request pump sees its {@code stopRequested} flag flip, finishes its current
 * iteration, unwinds the {@code try-with-resources} stack to flush the H2 sink + state store,
 * and the child exits with a clean status. We block on {@link Process#waitFor(long, TimeUnit)}
 * so the parent JVM does not race the child's checkpoint flush.
 */
final class FullProcessDbLogLauncher implements AutoCloseable {
  private static final ObjectMapper JSON = new ObjectMapper();

  /** System property used by the build to publish the bootJar archive path. */
  static final String JAR_PATH_PROPERTY = "dblog.test.jarPath";

  private final Process process;
  private final int controlPlanePort;
  private final HttpClient httpClient;
  private final Path logFile;

  private FullProcessDbLogLauncher(Process process, int controlPlanePort, Path logFile) {
    this.process = process;
    this.controlPlanePort = controlPlanePort;
    this.logFile = logFile;
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
  }

  static FullProcessDbLogLauncher launch(Map<String, String> properties, Path logFile)
      throws Exception {
    Map<String, String> finalProperties =
        new LinkedHashMap<>(Objects.requireNonNull(properties, "properties"));
    Objects.requireNonNull(logFile, "logFile");
    Files.createDirectories(logFile.getParent());

    // Race-free port discovery: ask the OS for a port via dblog.control-plane.port=0 and read the
    // bound port from the port-file the runtime publishes after binding. No pre-pick → no window
    // where a sibling fork could grab the same port between our peek and the child's bind.
    Path portFile = logFile.resolveSibling(logFile.getFileName() + ".port");
    Files.deleteIfExists(portFile);
    finalProperties.putIfAbsent("dblog.control-plane.enabled", "true");
    finalProperties.putIfAbsent("dblog.control-plane.host", "127.0.0.1");
    finalProperties.put("dblog.control-plane.port", "0");
    finalProperties.put("dblog.control-plane.port-file", portFile.toString());

    Path jarPath = resolveJarPath();
    List<String> command = new ArrayList<>();
    command.add(javaExecutable());
    command.add("-jar");
    command.add(jarPath.toString());
    for (Map.Entry<String, String> entry : finalProperties.entrySet()) {
      command.add("--" + entry.getKey() + "=" + entry.getValue());
    }

    ProcessBuilder processBuilder =
        new ProcessBuilder(command)
            .redirectErrorStream(true)
            .redirectOutput(logFile.toFile());
    Process process = processBuilder.start();
    int boundPort;
    try {
      boundPort = awaitBoundPort(portFile, process, logFile);
      waitForControlPlaneReady(boundPort, process, logFile);
    } catch (RuntimeException startupFailure) {
      destroyAndWait(process);
      throw startupFailure;
    }
    return new FullProcessDbLogLauncher(process, boundPort, logFile);
  }

  int controlPlanePort() {
    return controlPlanePort;
  }

  Path logFile() {
    return logFile;
  }

  String submitRequestId(
      String scope,
      String databaseName,
      String schemaName,
      String tableName,
      List<String> primaryKeyLiterals)
      throws Exception {
    Map<String, Object> response =
        submitRequest(scope, databaseName, schemaName, tableName, primaryKeyLiterals);
    Object request = response.get("request");
    if (!(request instanceof Map<?, ?> requestMap)) {
      throw new IllegalStateException("submit response missing 'request' object: " + response);
    }
    Object id = requestMap.get("requestId");
    if (id == null) {
      throw new IllegalStateException("submit response missing 'request.requestId': " + response);
    }
    return String.valueOf(id);
  }

  Map<String, Object> submitRequest(
      String scope,
      String databaseName,
      String schemaName,
      String tableName,
      List<String> primaryKeyLiterals)
      throws Exception {
    LinkedHashMap<String, Object> body = new LinkedHashMap<>();
    body.put("scope", Objects.requireNonNull(scope, "scope"));
    if (tableName != null) {
      LinkedHashMap<String, Object> table = new LinkedHashMap<>();
      table.put("databaseName", databaseName);
      table.put("schemaName", schemaName);
      table.put("tableName", tableName);
      body.put("table", table);
    }
    if (primaryKeyLiterals != null && !primaryKeyLiterals.isEmpty()) {
      body.put("primaryKeyLiterals", primaryKeyLiterals);
    }
    HttpResponse<String> response =
        httpClient.send(
            HttpRequest.newBuilder(uri("/api/v1/requests"))
                .header("Content-Type", "application/json")
                .timeout(MatrixTimeouts.HTTP_REQUEST)
                .POST(BodyPublishers.ofString(JSON.writeValueAsString(body)))
                .build(),
            BodyHandlers.ofString());
    if (response.statusCode() != 201) {
      throw new IllegalStateException(
          "submitRequest returned HTTP " + response.statusCode() + " body=" + response.body());
    }
    return JSON.readValue(response.body(), new TypeReference<Map<String, Object>>() {});
  }

  Map<String, Object> getRequest(String requestId) throws Exception {
    HttpResponse<String> response =
        httpClient.send(
            HttpRequest.newBuilder(uri("/api/v1/requests/" + requestId))
                .timeout(MatrixTimeouts.HTTP_REQUEST)
                .GET()
                .build(),
            BodyHandlers.ofString());
    if (response.statusCode() != 200) {
      throw new IllegalStateException(
          "getRequest returned HTTP " + response.statusCode() + " body=" + response.body());
    }
    return JSON.readValue(response.body(), new TypeReference<Map<String, Object>>() {});
  }

  void awaitRequestCompleted(String requestId, Duration timeout) throws Exception {
    Instant deadline = Instant.now().plus(timeout);
    Map<String, Object> latest = null;
    while (Instant.now().isBefore(deadline)) {
      ensureProcessAlive();
      latest = getRequest(requestId);
      String state = String.valueOf(latest.get("state"));
      if ("COMPLETED".equals(state)) {
        return;
      }
      if ("FAILED".equals(state)) {
        throw new IllegalStateException(
            "Request " + requestId + " ended in non-success state " + state + ": " + latest);
      }
      Thread.sleep(MatrixTimeouts.POLL_INTERVAL.toMillis());
    }
    throw new IllegalStateException(
        "Timed out waiting for request " + requestId + " to complete; last status=" + latest);
  }

  Map<String, Object> runtimeStatus() throws Exception {
    HttpResponse<String> response =
        httpClient.send(
            HttpRequest.newBuilder(uri("/api/v1/runtime/status"))
                .timeout(MatrixTimeouts.HTTP_REQUEST)
                .GET()
                .build(),
            BodyHandlers.ofString());
    if (response.statusCode() != 200) {
      throw new IllegalStateException(
          "runtimeStatus returned HTTP " + response.statusCode() + " body=" + response.body());
    }
    return JSON.readValue(response.body(), new TypeReference<Map<String, Object>>() {});
  }

  /**
   * Polls {@code /api/v1/runtime/status} until {@code sourceRuntime.lastAcknowledgedCheckpoint}
   * differs from {@code priorCheckpoint}. Used to confirm the streaming pump ingested a
   * source-side write that produced no in-flight request.
   */
  void awaitCheckpointAdvancePast(String priorCheckpoint, Duration timeout) throws Exception {
    Instant deadline = Instant.now().plus(timeout);
    String latest = null;
    while (Instant.now().isBefore(deadline)) {
      ensureProcessAlive();
      latest = currentCheckpoint();
      if (latest != null && !Objects.equals(latest, priorCheckpoint)) {
        return;
      }
      Thread.sleep(MatrixTimeouts.POLL_INTERVAL.toMillis());
    }
    throw new IllegalStateException(
        "Timed out waiting for checkpoint to advance past "
            + priorCheckpoint
            + " (latest="
            + latest
            + ")");
  }

  String currentCheckpoint() throws Exception {
    Map<String, Object> status = runtimeStatus();
    Object sourceRuntime = status.get("sourceRuntime");
    if (!(sourceRuntime instanceof Map<?, ?> sourceMap)) {
      return null;
    }
    Object value = sourceMap.get("lastAcknowledgedCheckpoint");
    return value == null ? null : String.valueOf(value);
  }

  @Override
  public void close() throws Exception {
    try {
      if (!process.isAlive()) {
        return;
      }
      process.destroy();
      boolean exited =
          process.waitFor(MatrixTimeouts.GRACEFUL_SHUTDOWN.toMillis(), TimeUnit.MILLISECONDS);
      if (!exited) {
        process.destroyForcibly();
        process.waitFor(5, TimeUnit.SECONDS);
        throw new IllegalStateException(
            "DBLog child process did not exit gracefully within "
                + MatrixTimeouts.GRACEFUL_SHUTDOWN
                + "; log file: "
                + logFile);
      }
    } finally {
      // JDK 21 HttpClient owns a selector thread + executor; both stay alive until JVM exit
      // unless explicitly closed. With ~16 launchers per compatibility-matrix run that adds up.
      httpClient.close();
    }
  }

  private void ensureProcessAlive() {
    if (!process.isAlive()) {
      throw new IllegalStateException(
          "DBLog child process exited unexpectedly with code "
              + process.exitValue()
              + "; log file: "
              + logFile);
    }
  }

  private URI uri(String path) {
    return URI.create("http://127.0.0.1:" + controlPlanePort + path);
  }

  private static void waitForControlPlaneReady(int port, Process process, Path logFile)
      throws Exception {
    URI healthUri = URI.create("http://127.0.0.1:" + port + "/api/v1/runtime/health");
    try (HttpClient client =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
      Instant deadline = Instant.now().plus(MatrixTimeouts.CONTROL_PLANE_READY);
      while (Instant.now().isBefore(deadline)) {
        if (!process.isAlive()) {
          throw new IllegalStateException(
              "DBLog child process exited (code "
                  + process.exitValue()
                  + ") before the control plane became ready on port "
                  + port
                  + "; log file: "
                  + logFile);
        }
        try {
          HttpResponse<String> response =
              client.send(
                  HttpRequest.newBuilder(healthUri)
                      .timeout(Duration.ofSeconds(2))
                      .GET()
                      .build(),
                  BodyHandlers.ofString());
          if (response.statusCode() == 200) {
            return;
          }
        } catch (IOException ignored) {
          // Server not yet listening.
        }
        Thread.sleep(MatrixTimeouts.CONTROL_PLANE_READY_POLL_INTERVAL.toMillis());
      }
      throw new IllegalStateException(
          "Control plane did not become ready on port "
              + port
              + " within "
              + MatrixTimeouts.CONTROL_PLANE_READY
              + "; log file: "
              + logFile);
    }
  }

  private static void destroyAndWait(Process process) {
    if (!process.isAlive()) {
      return;
    }
    process.destroy();
    try {
      if (!process.waitFor(MatrixTimeouts.GRACEFUL_SHUTDOWN.toMillis(), TimeUnit.MILLISECONDS)) {
        process.destroyForcibly();
        process.waitFor(5, TimeUnit.SECONDS);
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  private static Path resolveJarPath() {
    String configured = System.getProperty(JAR_PATH_PROPERTY);
    if (configured == null || configured.isBlank()) {
      throw new IllegalStateException(
          "System property "
              + JAR_PATH_PROPERTY
              + " is not set; the build is responsible for publishing the bootJar archive path"
              + " to the test JVM (see compatibilityMatrix task in build.gradle).");
    }
    Path path = Path.of(configured);
    if (!Files.isRegularFile(path)) {
      throw new IllegalStateException(
          "Configured DBLog bootJar path does not exist or is not a regular file: " + path);
    }
    return path;
  }

  private static String javaExecutable() {
    String javaHome = System.getProperty("java.home");
    Path candidate = Path.of(javaHome, "bin", "java");
    if (Files.isExecutable(candidate)) {
      return candidate.toString();
    }
    return "java";
  }

  private static int awaitBoundPort(Path portFile, Process process, Path logFile) throws Exception {
    Instant deadline = Instant.now().plus(MatrixTimeouts.CONTROL_PLANE_READY);
    while (Instant.now().isBefore(deadline)) {
      if (!process.isAlive()) {
        throw new IllegalStateException(
            "DBLog child process exited (code "
                + process.exitValue()
                + ") before publishing its bound control-plane port to "
                + portFile
                + "; log file: "
                + logFile);
      }
      if (Files.isRegularFile(portFile)) {
        String contents = Files.readString(portFile).strip();
        if (!contents.isEmpty()) {
          try {
            return Integer.parseInt(contents);
          } catch (NumberFormatException ignored) {
            // Reader saw the file mid-write (between Files.writeString temp + atomic move on a
            // filesystem that did not honour ATOMIC_MOVE). Retry — a clean value will follow.
          }
        }
      }
      Thread.sleep(MatrixTimeouts.CONTROL_PLANE_READY_POLL_INTERVAL.toMillis());
    }
    throw new IllegalStateException(
        "DBLog child process did not publish its control-plane port to "
            + portFile
            + " within "
            + MatrixTimeouts.CONTROL_PLANE_READY
            + "; log file: "
            + logFile);
  }
}
