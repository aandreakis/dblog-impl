package io.github.aandreakis.dblog.tap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP handler for {@code GET /api/v1/tap/stream}. Mounted by {@link
 * io.github.aandreakis.dblog.controlplane.http.ControlPlaneHttpServer} when a tap has
 * been attached.
 *
 * <p>The endpoint is chunked NDJSON, exactly one subscriber at a time; a new connection displaces
 * any existing one, matching TCP-like "last-wins" semantics. Pins the executor thread it runs on
 * for the duration of the subscription; that's deliberate since the tap is single-subscriber by
 * design, and TCP flow control is the backpressure mechanism that stalls the pump when the
 * subscriber can't keep up (step-mode).
 *
 * <h2>Delivery semantics: at-most-once per subscriber</h2>
 *
 * There is a narrow window between {@code queue.poll(...)} and a successful
 * {@code out.write(...) + out.flush(...)} in which a line exists only as a local {@code byte[]}
 * variable. If the client disconnects in that window — EPIPE on {@code write}, socket closed
 * during flush, TCP reset — the handler catches the {@code IOException}, logs at debug, and the
 * line is dropped: already popped from the queue, not delivered to anyone. The current
 * implementation does not re-enqueue on failure; a reconnecting reader starts from wherever the
 * queue head is at the time of reconnection.
 *
 * <p>A subscriber that is replaced by a new connection has the same shape: any line the old
 * handler had already popped but not yet flushed is written to the now-defunct old socket and
 * not seen by the new subscriber.
 *
 * <p>This is acceptable for the study-tool posture. If a future caller needs
 * exactly-once-per-attached-subscriber delivery, the intended fix is a peek-then-commit
 * inversion: peek the queue head, write, flush, and only {@code poll} after a clean flush. That
 * turns the semantics into at-least-once with possible duplicates on disconnect — readers would
 * need to dedupe by the envelope's monotonic {@code seq}. Design decision deferred until a
 * concrete consumer-side requirement exists.
 */
public final class TapHttpHandler {
  private static final Logger log = LoggerFactory.getLogger(TapHttpHandler.class);
  private static final long POLL_TIMEOUT_MS = 200L;
  private static final String TAP_NOT_ENABLED_MESSAGE =
      "Set dblog.tap.enabled=true to turn the tap on.";
  // Plain Jackson mapper for control-plane JSON error bodies. Separate from the tap event mapper
  // in TapObjectMapper so we don't carry the tap mixins / custom Instant serialiser into
  // unrelated HTTP error payloads.
  private static final ObjectMapper ERROR_JSON_MAPPER = new ObjectMapper();

  private final Tap tap;
  private final AtomicReference<Subscriber> active = new AtomicReference<>();

  public TapHttpHandler(Tap tap) {
    this.tap = Objects.requireNonNull(tap, "tap");
  }

  public void stream(HttpExchange exchange) throws IOException {
    if (!requireGet(exchange)) {
      return;
    }
    ActiveTap activeTap = resolveActiveTap(exchange);
    if (activeTap == null) {
      return;
    }

    Subscriber current = new Subscriber();
    Subscriber previous = active.getAndSet(current);
    if (previous != null) {
      previous.requestStop();
    }

    exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson; charset=utf-8");
    exchange.getResponseHeaders().set("Cache-Control", "no-store");
    // sendResponseHeaders(200, 0) triggers chunked transfer encoding — no Content-Length known.
    exchange.sendResponseHeaders(200, 0);

    TapQueue queue = activeTap.queue();
    try (OutputStream out = exchange.getResponseBody()) {
      long standbyEnteredNanos = -1L;
      while (!current.stopRequested()) {
        byte[] line;
        try {
          line = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          return;
        }
        if (line != null) {
          out.write(line);
          out.flush();
        }

        long fullSince = queue.queueFullSinceNanos();
        if (fullSince != -1L
            && standbyEnteredNanos == -1L
            && (System.nanoTime() - fullSince) >= activeTap.standbyThresholdNanos()) {
          standbyEnteredNanos = System.nanoTime();
          long fullMillis = (System.nanoTime() - fullSince) / 1_000_000L;
          byte[] standby = activeTap.buildStandbyPayload(fullMillis);
          out.write(standby);
          out.flush();
        }
        if (fullSince == -1L && standbyEnteredNanos != -1L) {
          long standbyMillis = (System.nanoTime() - standbyEnteredNanos) / 1_000_000L;
          byte[] resumed = activeTap.buildResumedPayload(standbyMillis);
          standbyEnteredNanos = -1L;
          out.write(resumed);
          out.flush();
        }
      }
    } catch (IOException ioe) {
      // Client disconnected mid-stream. That's the normal termination path.
      log.debug("tap subscriber disconnected: {}", ioe.getMessage());
    } finally {
      active.compareAndSet(current, null);
    }
  }

  /** Displace any in-flight subscriber, e.g. on control-plane shutdown. */
  public void stopAll() {
    Subscriber current = active.getAndSet(null);
    if (current != null) {
      current.requestStop();
    }
  }

  /**
   * Writes a 405 with {@code Allow: GET} and returns {@code false} if the request is not a GET;
   * otherwise returns {@code true}.
   */
  private static boolean requireGet(HttpExchange exchange) throws IOException {
    if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
      return true;
    }
    exchange.getResponseHeaders().set("Allow", "GET");
    writeJsonError(exchange, 405, "method_not_allowed", null);
    return false;
  }

  /**
   * Returns the underlying {@link ActiveTap} when the tap is enabled; otherwise writes a 503
   * and returns {@code null}.
   */
  private ActiveTap resolveActiveTap(HttpExchange exchange) throws IOException {
    if (tap instanceof ActiveTap activeTap) {
      return activeTap;
    }
    writeJsonError(exchange, 503, "tap_not_enabled", TAP_NOT_ENABLED_MESSAGE);
    return null;
  }

  private static void writeJsonError(
      HttpExchange exchange, int statusCode, String errorCode, String message) throws IOException {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("error", errorCode);
    if (message != null) {
      payload.put("message", message);
    }
    byte[] body;
    try {
      body = ERROR_JSON_MAPPER.writeValueAsBytes(payload);
    } catch (JsonProcessingException e) {
      throw new IOException("Failed to serialize tap error payload", e);
    }
    exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
    exchange.sendResponseHeaders(statusCode, body.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(body);
    }
  }

  private static final class Subscriber {
    private volatile boolean stopRequested;

    boolean stopRequested() {
      return stopRequested;
    }

    void requestStop() {
      stopRequested = true;
    }
  }
}
