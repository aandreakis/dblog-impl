package io.github.aandreakis.dblog.core.model;

import java.util.Arrays;

/**
 * Lightweight primary-key identity used as a {@link java.util.Map} key on the reconciliation
 * hot path. Carries only the normalised primary-key values (no column names, no literal, no
 * per-column wrapper objects), and compares equal to another instance when the value vectors
 * are structurally equal via {@link Arrays#deepEquals(Object[], Object[])}.
 *
 * <p>Intended to replace the heavier {@code PrimaryKeyTuple} as the reconciler's row-key type —
 * tuple stays around for progress literals, state-store persistence, and composite-PK parsing,
 * but the reconciliation map only needs identity, not a canonical literal.
 *
 * <p>Adapters pre-compute this at log-decode time from the values they already hold, so the
 * reconciler's per-event path allocates nothing beyond the hash wrapper itself.
 */
public final class PrimaryKeyHash {
  private final Object[] normalizedValues;
  private final int hashCode;

  private PrimaryKeyHash(Object[] normalizedValues) {
    this.normalizedValues = normalizedValues;
    this.hashCode = Arrays.deepHashCode(normalizedValues);
  }

  public static PrimaryKeyHash of(Object[] normalizedValues) {
    if (normalizedValues == null) {
      throw new IllegalArgumentException("normalizedValues must not be null");
    }
    if (normalizedValues.length == 0) {
      throw new IllegalArgumentException("normalizedValues must not be empty");
    }
    return new PrimaryKeyHash(normalizedValues.clone());
  }

  /** Wrap an {@code Object[]} by reference; caller must not mutate the array after the call. */
  public static PrimaryKeyHash wrapSharedArray(Object[] normalizedValues) {
    if (normalizedValues == null) {
      throw new IllegalArgumentException("normalizedValues must not be null");
    }
    if (normalizedValues.length == 0) {
      throw new IllegalArgumentException("normalizedValues must not be empty");
    }
    return new PrimaryKeyHash(normalizedValues);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof PrimaryKeyHash that)) {
      return false;
    }
    if (hashCode != that.hashCode) {
      return false;
    }
    return Arrays.deepEquals(normalizedValues, that.normalizedValues);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public String toString() {
    return "PrimaryKeyHash" + Arrays.deepToString(normalizedValues);
  }
}
