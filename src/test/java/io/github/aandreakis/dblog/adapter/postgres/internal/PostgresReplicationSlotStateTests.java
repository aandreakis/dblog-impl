package io.github.aandreakis.dblog.adapter.postgres.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.aandreakis.dblog.adapter.postgres.PostgresLsn;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Locks in the PostgreSQL replication-slot usability contract. This is the WAL/slot-purge
 * detection seam: if {@code pg_replication_slots.wal_status} is "lost" (PG reclaimed the
 * backing WAL segments) or {@code invalidation_reason} is populated (logical slot was
 * invalidated for any reason), {@link PostgresReplicationSlotState#isUsable()} must return
 * false so the slot manager refuses to reuse the slot.
 *
 * <p>DBLog intentionally does not attempt to auto-recover from a purged-WAL state — see
 * {@code docs/SPEC.md §4}. This test pins the detection boundary that produces the
 * operator-facing fail-closed error.
 */
class PostgresReplicationSlotStateTests {

  @ParameterizedTest(name = "[{index}] wal_status={0}, invalidation_reason={1} => usable={2}")
  @MethodSource("usabilityCases")
  void usabilityReflectsWalStatusAndInvalidationReason(
      String walStatus, String invalidationReason, boolean expectedUsable) {
    PostgresReplicationSlotState state =
        new PostgresReplicationSlotState(
            "dblog_slot",
            "logical",
            "appdb",
            "pgoutput",
            false,
            false,
            false,
            false,
            Optional.of(PostgresLsn.parse("0/16DA010")),
            Optional.of(PostgresLsn.parse("0/16DA018")),
            walStatus,
            invalidationReason);
    assertThat(state.isUsable()).isEqualTo(expectedUsable);
  }

  private static Stream<Arguments> usabilityCases() {
    return Stream.of(
        // Healthy slot: empty invalidation reason, wal_status that does not signal reclamation.
        Arguments.of("reserved", "", true),
        Arguments.of("extended", "", true),
        Arguments.of("unreserved", "", true),
        Arguments.of(null, null, true),
        Arguments.of("", "", true),
        // WAL reclaimed on the server: slot is no longer usable.
        Arguments.of("lost", "", false),
        // Slot explicitly invalidated by the server (e.g. `wal removed`, `rows removed` on PG17).
        Arguments.of("reserved", "wal_level insufficient", false),
        Arguments.of("", "wal removed", false),
        Arguments.of("", "rows removed", false),
        // Both signals present — still not usable.
        Arguments.of("lost", "wal removed", false));
  }

  @org.junit.jupiter.api.Test
  void invalidationReasonNormalizesNullToEmptyAndPreservesUsabilityContract() {
    PostgresReplicationSlotState state =
        new PostgresReplicationSlotState(
            "dblog_slot",
            "logical",
            "appdb",
            "pgoutput",
            false,
            false,
            false,
            false,
            Optional.empty(),
            Optional.empty(),
            null,
            null);
    assertThat(state.invalidationReason()).isEmpty();
    assertThat(state.walStatus()).isEmpty();
    assertThat(state.isUsable()).isTrue();
  }

  @org.junit.jupiter.api.Test
  void logicalSlotPredicateIsIndependentOfUsability() {
    // A slot can be "logical" yet not usable (e.g. WAL reclaimed). The two predicates must not
    // be conflated — the caller has to check both.
    PostgresReplicationSlotState lostLogicalSlot =
        new PostgresReplicationSlotState(
            "dblog_slot",
            "logical",
            "appdb",
            "pgoutput",
            false,
            false,
            false,
            false,
            Optional.empty(),
            Optional.empty(),
            "lost",
            "");
    assertThat(lostLogicalSlot.isLogical()).isTrue();
    assertThat(lostLogicalSlot.isUsable()).isFalse();
  }
}
