package io.github.aandreakis.dblog.core.request;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.PrimaryKeyTuple;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Locks in the {@link DumpStateCorruptionException} fail-closed invariants on
 * {@link DumpTableProgress}. These invariants protect restart safety: a corrupted
 * persisted partial-window state must fail closed rather than being silently "guessed
 * through" on restart (see docs/SPEC.md §20 and Phase 1.5 hardening in
 * docs/IMPLEMENTATION.md §11).
 *
 * <p>The test-author intent is one test per distinct throw site in
 * {@link DumpTableProgress}. If new invariants are added to the record, this class
 * should grow a matching test.
 */
class DumpTableProgressCorruptionTests {

  private static final TableSchema SCHEMA =
      TableSchema.create(
          new TableId("app", "public", "users"),
          List.of(
              new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
              new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
          Instant.parse("2026-04-10T00:00:00Z"));

  private static final String JOB_ID = "job-corrupt";
  private static final String TABLE = SCHEMA.tableId().displayName();
  private static final String FINGERPRINT = SCHEMA.fingerprint();

  private static PrimaryKeyTuple pk(String literal) {
    return SCHEMA.primaryKeyTupleFromLiteral(literal);
  }

  // -----------------------------------------------------------------------------------------
  // Canonical-constructor invariants (one test per distinct throw site).
  // -----------------------------------------------------------------------------------------

  @Test
  void rejectsPartialWatermarkStateWhenOnlyLowWatermarkIsPresent() {
    assertThatThrownBy(
            () ->
                new DumpTableProgress(
                    JOB_ID,
                    TABLE,
                    FINGERPRINT,
                    pk("9"),
                    pk("3"),
                    pk("3"),
                    "lw-only",
                    null,
                    false))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining("both active low and high watermark tokens must be present or absent");
  }

  @Test
  void rejectsPartialWatermarkStateWhenOnlyHighWatermarkIsPresent() {
    assertThatThrownBy(
            () ->
                new DumpTableProgress(
                    JOB_ID,
                    TABLE,
                    FINGERPRINT,
                    pk("9"),
                    pk("3"),
                    pk("3"),
                    null,
                    "hw-only",
                    false))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining("both active low and high watermark tokens must be present or absent");
  }

  @Test
  void rejectsActiveChunkStartAfterWithoutActiveWatermarkTokens() {
    assertThatThrownBy(
            () ->
                new DumpTableProgress(
                    JOB_ID,
                    TABLE,
                    FINGERPRINT,
                    pk("9"),
                    null,
                    pk("3"),
                    null,
                    null,
                    false))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining("activeChunkStartAfter cannot exist without active watermark tokens");
  }

  @Test
  void rejectsActiveChunkStartAfterWithoutRequestUpperBound() {
    assertThatThrownBy(
            () ->
                new DumpTableProgress(
                    JOB_ID,
                    TABLE,
                    FINGERPRINT,
                    null,
                    pk("3"),
                    pk("3"),
                    "lw",
                    "hw",
                    false))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining(
            "activeChunkStartAfter cannot exist without requestUpperBoundPrimaryKey");
  }

  @Test
  void rejectsIdenticalActiveLowAndHighWatermarkTokens() {
    assertThatThrownBy(
            () ->
                new DumpTableProgress(
                    JOB_ID,
                    TABLE,
                    FINGERPRINT,
                    pk("9"),
                    pk("3"),
                    pk("3"),
                    "same-token",
                    "same-token",
                    false))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining("active low and high watermark tokens must be distinct");
  }

  @Test
  void rejectsInFlightChunkWhenStartAfterDoesNotMatchLastCompleted() {
    assertThatThrownBy(
            () ->
                new DumpTableProgress(
                    JOB_ID,
                    TABLE,
                    FINGERPRINT,
                    pk("9"),
                    pk("3"),
                    pk("4"),
                    "lw",
                    "hw",
                    false))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining(
            "activeChunkStartAfter must match lastCompletedPrimaryKey for in-flight chunk retry");
  }

  @Test
  void rejectsCompletedStateWithoutLastCompletedPrimaryKey() {
    assertThatThrownBy(
            () ->
                new DumpTableProgress(
                    JOB_ID,
                    TABLE,
                    FINGERPRINT,
                    pk("9"),
                    null,
                    null,
                    null,
                    null,
                    true))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining("lastCompletedPrimaryKey is missing");
  }

  @Test
  void rejectsCompletedStateThatStillCarriesActiveWatermarkTokens() {
    // To isolate the "completed retains active watermark" guard, put a dummy low AND high
    // token so the earlier "low==high" / "partial pair" guards do not trigger first.
    // We also populate activeChunkStartAfter that matches lastCompleted so the boundary-
    // match guard passes; this leaves only the completed-still-has-active guard to fire.
    assertThatThrownBy(
            () ->
                new DumpTableProgress(
                    JOB_ID,
                    TABLE,
                    FINGERPRINT,
                    pk("9"),
                    pk("3"),
                    pk("3"),
                    "lw",
                    "hw",
                    true))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining("completed chunks must not retain active chunk state");
  }

  @Test
  void rejectsLastCompletedPrimaryKeyWithoutRequestUpperBound() {
    assertThatThrownBy(
            () ->
                new DumpTableProgress(
                    JOB_ID,
                    TABLE,
                    FINGERPRINT,
                    null,
                    pk("3"),
                    null,
                    null,
                    null,
                    false))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining("lastCompletedPrimaryKey requires requestUpperBoundPrimaryKey");
  }

  // -----------------------------------------------------------------------------------------
  // captureRequestUpperBound precondition invariants.
  // -----------------------------------------------------------------------------------------

  @Test
  void captureRequestUpperBoundRejectsReentryAfterChunkProgressRecorded() {
    DumpTableProgress initial =
        DumpTableProgress.initial(JOB_ID, TABLE, FINGERPRINT).captureRequestUpperBound(SCHEMA, "9");
    Chunk firstChunk =
        Chunk.fromMapRows(
            JOB_ID,
            TABLE,
            SCHEMA,
            null,
            List.of(Map.of("id", "1", "name", "a"), Map.of("id", "2", "name", "b")),
            "2",
            false);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw"), new WatermarkToken("hw"));
    DumpTableProgress completedFirstChunk =
        initial.beginChunk(firstChunk, window).completeChunk(firstChunk);

    assertThatThrownBy(() -> completedFirstChunk.captureRequestUpperBound(SCHEMA, "9"))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining(
            "requestUpperBoundPrimaryKey must be captured before chunk progress is recorded");
  }

  @Test
  void captureRequestUpperBoundRejectsReentryWhileAnActiveChunkIsInFlight() {
    DumpTableProgress initial =
        DumpTableProgress.initial(JOB_ID, TABLE, FINGERPRINT).captureRequestUpperBound(SCHEMA, "9");
    Chunk firstChunk =
        Chunk.fromMapRows(
            JOB_ID,
            TABLE,
            SCHEMA,
            null,
            List.of(Map.of("id", "1", "name", "a")),
            "1",
            false);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw"), new WatermarkToken("hw"));
    DumpTableProgress active = initial.beginChunk(firstChunk, window);

    assertThat(active.hasActiveChunk()).isTrue();
    assertThatThrownBy(() -> active.captureRequestUpperBound(SCHEMA, "9"))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining(
            "requestUpperBoundPrimaryKey must be captured before chunk progress is recorded");
  }

  // -----------------------------------------------------------------------------------------
  // verifyChunkMatchesProgress invariants (exercised through beginChunk / completeChunk).
  // -----------------------------------------------------------------------------------------

  @Test
  void beginChunkFailsClosedWhenRequestUpperBoundWasNeverCaptured() {
    DumpTableProgress progressWithoutUpperBound =
        DumpTableProgress.initial(JOB_ID, TABLE, FINGERPRINT);
    Chunk chunk =
        Chunk.fromMapRows(
            JOB_ID,
            TABLE,
            SCHEMA,
            null,
            List.of(Map.of("id", "1", "name", "a")),
            "1",
            false);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw"), new WatermarkToken("hw"));

    assertThatThrownBy(() -> progressWithoutUpperBound.beginChunk(chunk, window))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining("chunk verification requires a persisted requestUpperBoundPrimaryKey");
  }

  @Test
  void beginChunkFailsClosedWhenChunkStartAfterReachesOrExceedsRequestUpperBound() {
    DumpTableProgress progress =
        DumpTableProgress.initial(JOB_ID, TABLE, FINGERPRINT).captureRequestUpperBound(SCHEMA, "5");
    // startAfter == upper bound (>= comparison fires at equality too)
    Chunk chunkAtUpperBound =
        Chunk.fromMapRows(
            JOB_ID,
            TABLE,
            SCHEMA,
            "5",
            List.of(Map.of("id", "6", "name", "too-far")),
            "6",
            false);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw"), new WatermarkToken("hw"));

    assertThatThrownBy(() -> progress.beginChunk(chunkAtUpperBound, window))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining("startAfterPrimaryKey must remain below requestUpperBoundPrimaryKey");
  }

  @Test
  void beginChunkFailsClosedWhenChunkLastPrimaryKeyExceedsRequestUpperBound() {
    DumpTableProgress progress =
        DumpTableProgress.initial(JOB_ID, TABLE, FINGERPRINT).captureRequestUpperBound(SCHEMA, "5");
    // chunk selects rows up through "7" which is beyond the captured upper bound "5".
    Chunk chunkPastUpperBound =
        Chunk.fromMapRows(
            JOB_ID,
            TABLE,
            SCHEMA,
            null,
            List.of(Map.of("id", "1", "name", "a"), Map.of("id", "7", "name", "past")),
            "7",
            false);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw"), new WatermarkToken("hw"));

    assertThatThrownBy(() -> progress.beginChunk(chunkPastUpperBound, window))
        .isInstanceOf(DumpStateCorruptionException.class)
        .hasMessageContaining("lastPrimaryKey exceeded requestUpperBoundPrimaryKey");
  }

  // -----------------------------------------------------------------------------------------
  // Happy-path sanity: the round-trip that the invariants above protect still works.
  // -----------------------------------------------------------------------------------------

  @Test
  void happyPathBeginAndCompleteChunkPreservesEveryInvariant() {
    DumpTableProgress initial =
        DumpTableProgress.initial(JOB_ID, TABLE, FINGERPRINT).captureRequestUpperBound(SCHEMA, "5");
    Chunk chunk =
        Chunk.fromMapRows(
            JOB_ID,
            TABLE,
            SCHEMA,
            null,
            List.of(Map.of("id", "1", "name", "a"), Map.of("id", "2", "name", "b")),
            "2",
            false);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw"), new WatermarkToken("hw"));

    DumpTableProgress active = initial.beginChunk(chunk, window);
    DumpTableProgress completed = active.completeChunk(chunk);

    assertThat(active.hasActiveChunk()).isTrue();
    assertThat(active.activeChunkStartAfterTuple()).isNull();
    assertThat(active.activeLowWatermark()).isEqualTo("lw");
    assertThat(active.activeHighWatermark()).isEqualTo("hw");

    assertThat(completed.chunkCompleted()).isTrue();
    assertThat(completed.hasActiveChunk()).isFalse();
    assertThat(completed.lastCompletedPrimaryKey()).isEqualTo("2");
    assertThat(completed.activeLowWatermark()).isNull();
    assertThat(completed.activeHighWatermark()).isNull();
  }
}
