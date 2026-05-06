package io.github.aandreakis.dblog.integration.versionmatrix;

import java.time.Duration;

/**
 * Single home for every wall-clock-shaped value in the version-matrix tests, each with a one-line
 * rationale. Keeping them here means a slow-CI tuning pass touches one file instead of grep'ing
 * the package.
 *
 * <p>Two mental categories:
 *
 * <ul>
 *   <li><strong>Launch / shutdown</strong> — bound by JVM cold start, Spring bootstrap, and the
 *       child's clean shutdown sequence. These are generous so the first run of the day on a
 *       cold cache passes without flaking.
 *   <li><strong>In-test polling</strong> — bound by the real production code's expected latency
 *       (binlog/pgoutput tail + dump-window coordination + checkpoint flush) plus a comfortable
 *       multiplier for slow CI.
 * </ul>
 */
final class MatrixTimeouts {
  private MatrixTimeouts() {}

  // --- Launch / shutdown ---------------------------------------------------------------------

  /** JVM cold start + Spring init + adapter's first JDBC connection — generous for slow CI. */
  static final Duration CONTROL_PLANE_READY = Duration.ofSeconds(120);

  /** Trades readiness latency against HTTP-probe rate during the cold-start spin loop. */
  static final Duration CONTROL_PLANE_READY_POLL_INTERVAL = Duration.ofMillis(250);

  /** Headroom for the child to finish its in-flight H2 checkpoint flush and close the sink. */
  static final Duration GRACEFUL_SHUTDOWN = Duration.ofSeconds(30);

  // --- HTTP --------------------------------------------------------------------------------

  /** Per-call ceiling for a single control-plane HTTP request — should be sub-second normally. */
  static final Duration HTTP_REQUEST = Duration.ofSeconds(10);

  // --- In-test polling --------------------------------------------------------------------

  /** Small dumps complete in &lt; 1 s on a fast box; this is the slow-CI ceiling. */
  static final Duration DUMP_REQUEST_COMPLETION = Duration.ofSeconds(60);

  /** Binlog / pgoutput read latency + checkpoint dispatcher's batched flush window. */
  static final Duration STREAMING_CHECKPOINT_ADVANCE = Duration.ofSeconds(30);

  /** The streaming pump fires its first heartbeat &lt; 1 s after startup; this is jitter padding. */
  static final Duration HEARTBEAT_POPULATED = Duration.ofSeconds(15);

  /** Generic in-test polling cadence — keeps loops responsive without thrashing. */
  static final Duration POLL_INTERVAL = Duration.ofMillis(150);
}
