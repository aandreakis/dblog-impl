package io.github.aandreakis.dblog.core.model;

/**
 * Opaque, per-adapter typed source-log position.
 *
 * <p>Algorithm 1 needs only monotonic ordering of committed transactions within a single source
 * — it never compares positions across sources, never decomposes them, and never assumes a
 * specific binary shape. Each adapter therefore ships its own implementation typed to that
 * source's native log shape (MySQL binlog file/position with optional GTID set, PostgreSQL
 * 64-bit LSN, etc.) and provides its own {@link Comparable} relation. A uniform {@code long}
 * cannot losslessly cover every vendor (SQL Server's 10-byte LSN, Oracle SCNs &gt; 2<sup>63</sup>),
 * which is why DBLog deliberately keeps this surface opaque.
 *
 * <p>Adapter checkpoint-position implementations are expected to be value-typed
 * (record-shaped), immutable, and {@link Comparable} to themselves. Cross-implementation
 * comparison is not defined. Non-checkpoint helpers such as {@link OpaqueSourcePosition} may
 * implement only the display surface.
 */
public interface SourcePosition {
  /**
   * Stable string form used for operator-facing display (logs, runtime status payloads, tap event
   * envelopes). For adapter checkpoint positions, this string is also used by the corresponding
   * {@code SourceCheckpointCodec}: the codec encodes via {@code displayValue()} and decodes via
   * the implementation's static {@code parse(String)} method, so the format is the de facto
   * on-disk checkpoint contract. Checkpoint-position implementations must therefore:
   *
   * <ul>
   *   <li>provide a static {@code parse(String)} that round-trips with {@code displayValue()};
   *   <li>preserve format stability across releases (a format change requires a coordinated
   *       state-store reset — DBLog does not auto-migrate stored checkpoints).
   * </ul>
   */
  String displayValue();
}
