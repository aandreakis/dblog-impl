package io.github.aandreakis.dblog.adapter.api;

/**
 * Per-dialect port for decoding raw wire-format values that the neutral value normalizer cannot
 * interpret on its own. The only current leak is MySQL's binary JSON wire format, but this port
 * is also the seam for future dialect-specific raw forms (Oracle JSON, SQL Server {@code
 * hierarchyid}, PostgreSQL custom binary encodings, …).
 *
 * <p>Return values must be in a form the neutral normalizer can pass through unchanged — e.g.
 * a decoded {@code String} for JSON.
 */
public interface ValueDecoder {
  /**
   * Pass-through decoder for dialects whose wire format requires no dialect-specific decoding
   * (e.g. text-only pgoutput).
   */
  ValueDecoder PASS_THROUGH = raw -> raw;

  /**
   * Decode a raw JSON value from this dialect's wire format. Implementations that have no special
   * form to decode may simply return {@code raw}.
   */
  Object decodeJson(Object raw);
}
