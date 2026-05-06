package io.github.aandreakis.dblog.core.request;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.aandreakis.dblog.core.model.TableId;
import io.github.aandreakis.dblog.core.model.WatermarkToken;
import io.github.aandreakis.dblog.core.reconcile.Chunk;
import io.github.aandreakis.dblog.core.reconcile.WatermarkWindow;
import io.github.aandreakis.dblog.core.schema.ColumnDefinition;
import io.github.aandreakis.dblog.core.schema.NeutralColumnType;
import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DumpRequestModelTests {
  private static final TableSchema USER_SCHEMA =
      TableSchema.create(
          new TableId("app", "public", "users"),
          List.of(
              new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
              new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
          Instant.parse("2026-04-10T00:00:00Z"));

  @Test
  void requestStatusRejectsMissingKeysForNonPrimaryKeyRequests() {
    DumpRequest request =
        DumpRequest.fromPrimaryKeyLiterals(
            "repair-1",
            DumpScope.PRIMARY_KEYS,
            USER_SCHEMA.tableId(),
            USER_SCHEMA,
            List.of("42"));

    DumpRequestStatus active = DumpRequestStatus.active(request);

    assertThat(active.state()).isEqualTo(DumpRequestState.ACTIVE);

    assertThatThrownBy(
            () ->
                new DumpRequestStatus(
                    request.requestId(),
                    DumpScope.TABLE,
                    request.tableId(),
                    DumpRequestState.COMPLETED,
                    request.primaryKeyTuples(),
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("only primary-key repair");
  }

  @Test
  void primaryKeyRequestsAreBoundedToFiveHundredKeys() {
    List<String> keys =
        java.util.stream.IntStream.rangeClosed(1, DumpRequest.MAX_PRIMARY_KEY_LITERALS + 1)
            .mapToObj(Integer::toString)
            .toList();

    assertThatThrownBy(
            () ->
                new DumpRequest(
                    "repair-oversized",
                    DumpScope.PRIMARY_KEYS,
                    USER_SCHEMA.tableId(),
                    USER_SCHEMA.primaryKeyTuplesFromLiterals(keys)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at most " + DumpRequest.MAX_PRIMARY_KEY_LITERALS);
  }

  @Test
  void dumpProgressCapturesRestartBoundariesConservatively() {
    TableSchema schema =
        TableSchema.create(
            new TableId("app", "public", "users"),
            List.of(
                new ColumnDefinition("id", "bigint", NeutralColumnType.INTEGER, true, false),
                new ColumnDefinition("name", "text", NeutralColumnType.STRING, false, true)),
            Instant.parse("2026-04-10T00:00:00Z"));
    Chunk chunk =
        Chunk.fromMapRows(
            "job-1",
            schema.tableId().displayName(),
            schema,
            null,
            List.of(Map.of("id", "1", "name", "a"), Map.of("id", "2", "name", "b")),
            "2",
            false);
    WatermarkWindow window =
        new WatermarkWindow(new WatermarkToken("lw"), new WatermarkToken("hw"));

    DumpTableProgress initial =
        DumpTableProgress.initial("job-1", schema.tableId().displayName(), schema.fingerprint())
            .captureRequestUpperBound(schema, "5");
    DumpTableProgress active = initial.beginChunk(chunk, window);
    DumpTableProgress completed = active.completeChunk(chunk);

    assertThat(active.hasActiveChunk()).isTrue();
    assertThat(active.restartFromLastCompletedBoundary().hasActiveChunk()).isFalse();
    assertThat(completed.chunkCompleted()).isTrue();
    assertThat(completed.lastCompletedPrimaryKey()).isEqualTo("2");
  }
}
