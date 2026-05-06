package io.github.aandreakis.dblog.controlplane.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.github.aandreakis.dblog.controlplane.service.ControlPlaneCommandService;
import io.github.aandreakis.dblog.controlplane.service.ControlPlaneQueryService;
import io.github.aandreakis.dblog.core.request.DumpRequest;
import io.github.aandreakis.dblog.state.api.DumpRequestNotFoundException;
import io.github.aandreakis.dblog.tap.TapHttpHandler;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class ControlPlaneHttpServer {
  private static final org.slf4j.Logger log =
      org.slf4j.LoggerFactory.getLogger(ControlPlaneHttpServer.class);
  private static final int DEFAULT_EXECUTOR_MAX_THREADS = 8;
  private static final int DEFAULT_EXECUTOR_QUEUE_CAPACITY = 64;
  private static final int DEFAULT_MAX_REQUEST_BODY_BYTES = 1_048_576;

  private final ControlPlaneQueryService queryService;
  private final ControlPlaneCommandService commandService;
  private final String host;
  private final int port;
  private final boolean allowNonLoopback;
  private final int executorMaxThreads;
  private final int executorQueueCapacity;
  private final int maxRequestBodyBytes;

  private volatile HttpServer server;
  private volatile ExecutorService executor;
  private volatile int boundPort = -1;
  private volatile TapHttpHandler tapHttpHandler;

  public ControlPlaneHttpServer(
      ControlPlaneQueryService queryService,
      ControlPlaneCommandService commandService,
      String host,
      int port) {
    this(
        queryService,
        commandService,
        host,
        port,
        false,
        DEFAULT_EXECUTOR_MAX_THREADS,
        DEFAULT_EXECUTOR_QUEUE_CAPACITY,
        DEFAULT_MAX_REQUEST_BODY_BYTES);
  }

  public ControlPlaneHttpServer(
      ControlPlaneQueryService queryService,
      ControlPlaneCommandService commandService,
      String host,
      int port,
      boolean allowNonLoopback,
      int executorMaxThreads,
      int executorQueueCapacity,
      int maxRequestBodyBytes) {
    this.queryService = Objects.requireNonNull(queryService, "queryService");
    this.commandService = Objects.requireNonNull(commandService, "commandService");
    this.host = Objects.requireNonNull(host, "host");
    this.port = port;
    this.allowNonLoopback = allowNonLoopback;
    if (executorMaxThreads <= 0) {
      throw new IllegalArgumentException("executorMaxThreads must be > 0");
    }
    if (executorQueueCapacity <= 0) {
      throw new IllegalArgumentException("executorQueueCapacity must be > 0");
    }
    if (maxRequestBodyBytes <= 0) {
      throw new IllegalArgumentException("maxRequestBodyBytes must be > 0");
    }
    this.executorMaxThreads = executorMaxThreads;
    this.executorQueueCapacity = executorQueueCapacity;
    this.maxRequestBodyBytes = maxRequestBodyBytes;
  }

  public ControlPlaneQueryService queryService() {
    return queryService;
  }

  public ControlPlaneCommandService commandService() {
    return commandService;
  }

  public synchronized void start() {
    if (server != null) {
      return;
    }
    enforceLoopbackPolicy();
    try {
      HttpServer localServer = HttpServer.create(new InetSocketAddress(host, port), 0);
      localServer.createContext("/api/v1", new ApiHandler());
      ThreadPoolExecutor localExecutor =
          new ThreadPoolExecutor(
              executorMaxThreads,
              executorMaxThreads,
              60L,
              TimeUnit.SECONDS,
              new ArrayBlockingQueue<>(executorQueueCapacity),
              Thread.ofPlatform().name("dblog-controlplane-http-", 0).factory(),
              new ThreadPoolExecutor.CallerRunsPolicy());
      localExecutor.allowCoreThreadTimeOut(true);
      localServer.setExecutor(localExecutor);
      localServer.start();
      this.server = localServer;
      this.executor = localExecutor;
      this.boundPort = localServer.getAddress().getPort();
    } catch (IOException e) {
      throw new IllegalStateException("failed to start control-plane HTTP server", e);
    }
  }

  public synchronized void stop() {
    TapHttpHandler localTap = tapHttpHandler;
    if (localTap != null) {
      localTap.stopAll();
    }
    HttpServer localServer = server;
    if (localServer != null) {
      localServer.stop(0);
    }
    ExecutorService localExecutor = executor;
    if (localExecutor != null) {
      localExecutor.shutdownNow();
    }
    server = null;
    executor = null;
    boundPort = -1;
  }

  /**
   * Attaches the educational observability tap to the control-plane HTTP server. Must be called
   * before {@link #start()}. Passing {@code null} detaches a previously attached handler.
   */
  public synchronized void attachTap(TapHttpHandler handler) {
    if (server != null) {
      throw new IllegalStateException("attach tap before start()");
    }
    this.tapHttpHandler = handler;
  }

  public int boundPort() {
    return boundPort;
  }

  /**
   * Refuses to bind a non-loopback address unless {@code dblog.control-plane.allow-non-loopback=true}
   * is set explicitly. The control plane does not authenticate callers; exposing it on a
   * non-loopback interface without operator intent is a known footgun. Containerized deployments
   * that rely on Docker port-publishing for host-side isolation must set the opt-in.
   */
  private void enforceLoopbackPolicy() {
    InetAddress resolved;
    try {
      resolved = InetAddress.getByName(host);
    } catch (UnknownHostException ex) {
      throw new IllegalStateException(
          "control plane host '"
              + host
              + "' could not be resolved. Set dblog.control-plane.host to a loopback address"
              + " (127.0.0.1, localhost, or ::1) or a literal address of an interface on this"
              + " host.",
          ex);
    }
    if (resolved.isLoopbackAddress()) {
      return;
    }
    if (allowNonLoopback) {
      log.warn(
          "control plane binding non-loopback host={} port={} — authentication is NOT performed;"
              + " only the enclosing container or network layer controls access",
          host,
          port);
      return;
    }
    throw new IllegalStateException(
        "control plane host '"
            + host
            + "' is not a loopback address. The control plane does not authenticate callers and"
            + " must not be exposed to the network without explicit opt-in. Set"
            + " dblog.control-plane.allow-non-loopback=true to bind non-loopback (intended for"
            + " containerized deployments that rely on external port publishing for isolation),"
            + " or change dblog.control-plane.host to a loopback address.");
  }

  private final class ApiHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      try {
        URI requestUri = exchange.getRequestURI();
        String path = normalizeRoutePath(requestUri.getPath());
        Map<String, String> query = parseQuery(requestUri);

        if (path.equals("/api/v1/tap/stream")) {
          TapHttpHandler handler = tapHttpHandler;
          if (handler == null) {
            writeJson(
                exchange,
                503,
                Map.of(
                    "error",
                    "tap_not_enabled",
                    "message",
                    "The educational observability tap is not enabled on this runtime (set"
                        + " dblog.tap.enabled=true to turn it on)."));
            return;
          }
          handler.stream(exchange);
          return;
        }
        if (path.equals("/api/v1/runtime")) {
          requireMethod(exchange, "GET");
          writeJson(exchange, 200, queryService.runtimePayload());
          return;
        }
        if (path.equals("/api/v1/runtime/health")) {
          requireMethod(exchange, "GET");
          writeJson(exchange, 200, queryService.healthPayload());
          return;
        }
        if (path.equals("/api/v1/runtime/status")) {
          requireMethod(exchange, "GET");
          writeJson(exchange, 200, queryService.runtimeStatusPayload());
          return;
        }
        if (path.equals("/api/v1/metrics")) {
          requireMethod(exchange, "GET");
          writeJson(exchange, 200, queryService.metricsPayload());
          return;
        }
        if (path.equals("/api/v1/runtime/schemas")) {
          requireMethod(exchange, "GET");
          writeJson(exchange, 200, queryService.runtimeSchemasPayload());
          return;
        }
        if (path.equals("/api/v1/runtime/schema-issues")) {
          requireMethod(exchange, "GET");
          writeJson(exchange, 200, queryService.runtimeSchemaIssuesPayload());
          return;
        }
        if (path.equals("/api/v1/events/recent")) {
          requireMethod(exchange, "GET");
          int limit = parseLimitQueryParam(query, 20);
          writeJson(exchange, 200, queryService.recentEventsPayload(limit));
          return;
        }
        if (path.equals("/api/v1/events/summary")) {
          requireMethod(exchange, "GET");
          writeJson(exchange, 200, queryService.eventSummaryPayload());
          return;
        }
        if (path.startsWith("/api/v1/events/tables/")) {
          requireMethod(exchange, "GET");
          int limit = parseLimitQueryParam(query, 10);
          String tableDisplayName = urlDecode(path.substring("/api/v1/events/tables/".length()));
          writeJson(exchange, 200, queryService.tableEventsPayload(tableDisplayName, limit));
          return;
        }
        if (path.equals("/api/v1/requests")) {
          if (exchange.getRequestMethod().equalsIgnoreCase("GET")) {
            Integer limit = query.containsKey("limit") ? parseLimitQueryParam(query, 0) : null;
            writeJson(exchange, 200, queryService.requestsPayload(query.get("state"), limit));
            return;
          }
          if (exchange.getRequestMethod().equalsIgnoreCase("POST")) {
            Map<String, Object> payload = parseJsonObjectBody(exchange);
            DumpRequest request = commandService.submit(toRequestPayload(payload));
            writeJson(
                exchange,
                201,
                Map.of(
                    "accepted", true,
                    "request", queryService.requestPayload(request.requestId())));
            return;
          }
          throw MethodNotAllowedException.forMethods(exchange.getRequestMethod(), "GET", "POST");
        }
        if (path.startsWith("/api/v1/requests/")) {
          String suffix = path.substring("/api/v1/requests/".length());
          requireMethod(exchange, "GET");
          writeJson(exchange, 200, queryService.requestPayload(urlDecode(suffix)));
          return;
        }

        writeJson(exchange, 404, Map.of("error", "not_found", "message", "No route matches " + path));
      } catch (MethodNotAllowedException e) {
        writeError(exchange, 405, "method_not_allowed", e.getMessage(), e.allowHeaderValue());
      } catch (DumpRequestNotFoundException e) {
        writeError(exchange, 404, "not_found", e.getMessage(), null);
      } catch (ControlPlaneCommandService.RequestSubmissionUnavailableException e) {
        writeError(exchange, 503, "service_unavailable", e.getMessage(), null);
      } catch (RequestTooLargeException e) {
        writeError(exchange, 413, "request_too_large", e.getMessage(), null);
      } catch (IllegalArgumentException e) {
        writeError(exchange, 400, "bad_request", e.getMessage(), null);
      } catch (RuntimeException e) {
        writeError(
            exchange,
            500,
            "internal_error",
            e.getMessage() == null ? "Unexpected internal failure" : e.getMessage(),
            null);
      } finally {
        exchange.close();
      }
    }
  }

  private void requireMethod(HttpExchange exchange, String method) {
    if (!exchange.getRequestMethod().equalsIgnoreCase(method)) {
      throw MethodNotAllowedException.forMethods(exchange.getRequestMethod(), method);
    }
  }

  /**
   * Normalises a request path for route matching. Currently strips a single trailing
   * slash when the path is not the bare root, so {@code /api/v1/runtime/} matches the
   * same route as {@code /api/v1/runtime}. Previously trailing slashes produced a 404
   * because every route used strict {@code equals(...)} matching.
   *
   * <p>The raw path is still available via {@code exchange.getRequestURI().getPath()}
   * if a future handler needs to distinguish — this helper only affects routing.
   */
  private static String normalizeRoutePath(String path) {
    if (path == null || path.length() <= 1) {
      return path;
    }
    if (path.endsWith("/")) {
      return path.substring(0, path.length() - 1);
    }
    return path;
  }

  /**
   * Parses the {@code limit} query parameter as a non-negative integer, returning
   * {@code defaultLimit} when the caller omitted it. Negative values and non-numeric
   * text fail with {@link IllegalArgumentException}, which the outer handler maps to
   * {@code 400 bad_request}. Previously a {@code limit=-1} silently accepted as "no
   * limit" and returned an empty result — confusing for a client expecting either a
   * validation error or a capped response.
   */
  private static int parseLimitQueryParam(Map<String, String> query, int defaultLimit) {
    String raw = query.get("limit");
    if (raw == null) {
      return defaultLimit;
    }
    int value;
    try {
      value = Integer.parseInt(raw);
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("limit must be a non-negative integer, got: " + raw);
    }
    if (value < 0) {
      throw new IllegalArgumentException("limit must be non-negative, got: " + value);
    }
    return value;
  }

  private void writeJson(HttpExchange exchange, int statusCode, Object payload) throws IOException {
    byte[] body = JsonCodec.encode(payload).getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
    exchange.sendResponseHeaders(statusCode, body.length);
    exchange.getResponseBody().write(body);
  }

  private void writeError(
      HttpExchange exchange,
      int statusCode,
      String errorCode,
      String message,
      String allowHeaderValue)
      throws IOException {
    if (allowHeaderValue != null) {
      exchange.getResponseHeaders().set("Allow", allowHeaderValue);
    }
    writeJson(
        exchange,
        statusCode,
        Map.of(
            "error", errorCode,
            "message", message == null ? "" : message));
  }

  private String readBody(HttpExchange exchange) throws IOException {
    try (InputStream inputStream = exchange.getRequestBody()) {
      int declaredLength = parseContentLength(exchange);
      if (declaredLength > maxRequestBodyBytes) {
        throw new RequestTooLargeException(
            "request body exceeds the configured control-plane limit of "
                + maxRequestBodyBytes
                + " bytes");
      }
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      byte[] chunk = new byte[8192];
      int total = 0;
      int read;
      while ((read = inputStream.read(chunk)) > 0) {
        total += read;
        if (total > maxRequestBodyBytes) {
          throw new RequestTooLargeException(
              "request body exceeds the configured control-plane limit of "
                  + maxRequestBodyBytes
                  + " bytes");
        }
        buffer.write(chunk, 0, read);
      }
      return buffer.toString(StandardCharsets.UTF_8);
    }
  }

  private int parseContentLength(HttpExchange exchange) {
    String raw = exchange.getRequestHeaders().getFirst("Content-Length");
    if (raw == null || raw.isBlank()) {
      return -1;
    }
    try {
      return Integer.parseInt(raw.trim());
    } catch (NumberFormatException ignored) {
      return -1;
    }
  }

  private Map<String, Object> parseJsonObjectBody(HttpExchange exchange) throws IOException {
    Object parsed;
    try {
      parsed = JsonCodec.parse(readBody(exchange));
    } catch (RequestTooLargeException failure) {
      throw failure;
    } catch (RuntimeException failure) {
      throw new IllegalArgumentException("malformed JSON", failure);
    }
    if (!(parsed instanceof Map<?, ?> rawMap)) {
      throw new IllegalArgumentException("request body must be a JSON object");
    }
    LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
      payload.put(String.valueOf(entry.getKey()), entry.getValue());
    }
    return payload;
  }

  private ControlPlaneCommandService.RequestPayload toRequestPayload(Map<String, Object> payload) {
    Objects.requireNonNull(payload, "payload");
    Object rawTable = payload.get("table");
    if (rawTable != null && !(rawTable instanceof Map<?, ?>)) {
      throw new IllegalArgumentException("table must be a JSON object when provided");
    }
    Object rawPrimaryKeyLiterals = payload.get("primaryKeyLiterals");
    if (rawPrimaryKeyLiterals != null && !(rawPrimaryKeyLiterals instanceof List<?>)) {
      throw new IllegalArgumentException("primaryKeyLiterals must be a JSON array when provided");
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> table = (Map<String, Object>) rawTable;
    List<String> primaryKeyLiterals =
        rawPrimaryKeyLiterals instanceof List<?> values
            ? values.stream().map(String::valueOf).toList()
            : List.of();
    return new ControlPlaneCommandService.RequestPayload(
        payload.get("scope") == null ? null : String.valueOf(payload.get("scope")),
        table == null
            ? null
            : new ControlPlaneCommandService.TablePayload(
                stringValue(table.get("databaseName")),
                stringValue(table.get("schemaName")),
                stringValue(table.get("tableName"))),
        primaryKeyLiterals);
  }

  private static String stringValue(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private static Map<String, String> parseQuery(URI uri) {
    LinkedHashMap<String, String> query = new LinkedHashMap<>();
    String rawQuery = uri.getRawQuery();
    if (rawQuery == null || rawQuery.isBlank()) {
      return query;
    }
    for (String pair : rawQuery.split("&")) {
      if (pair.isBlank()) {
        continue;
      }
      int separator = pair.indexOf('=');
      if (separator < 0) {
        query.put(urlDecode(pair), "");
      } else {
        query.put(urlDecode(pair.substring(0, separator)), urlDecode(pair.substring(separator + 1)));
      }
    }
    return query;
  }

  private static String urlDecode(String value) {
    return java.net.URLDecoder.decode(value, StandardCharsets.UTF_8);
  }

  private static final class MethodNotAllowedException extends RuntimeException {
    private final String allowHeaderValue;

    private MethodNotAllowedException(String message, String allowHeaderValue) {
      super(message);
      this.allowHeaderValue = allowHeaderValue;
    }

    private String allowHeaderValue() {
      return allowHeaderValue;
    }

    private static MethodNotAllowedException forMethods(String actualMethod, String... allowedMethods) {
      String allowValue = String.join(", ", allowedMethods);
      String allowedLabel = String.join(" or ", allowedMethods);
      return new MethodNotAllowedException(
          "Expected " + allowedLabel + " but received " + actualMethod, allowValue);
    }
  }

  private static final class RequestTooLargeException extends RuntimeException {
    private RequestTooLargeException(String message) {
      super(message);
    }
  }
}
