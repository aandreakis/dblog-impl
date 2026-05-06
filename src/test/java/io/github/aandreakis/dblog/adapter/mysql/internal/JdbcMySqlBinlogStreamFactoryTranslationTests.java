package io.github.aandreakis.dblog.adapter.mysql.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.MySqlGtid;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class JdbcMySqlBinlogStreamFactoryTranslationTests {
  private static final Instant TX_TIME = Instant.parse("2026-03-21T00:00:00Z");

  @Test
  void translatesTableMapUsingClientFilenameFallbackAndCopiesMetadata() throws Exception {
    TableMapEventData tableMap = mock(TableMapEventData.class);
    TableMapEventMetadata metadata = mock(TableMapEventMetadata.class);
    byte[] columnTypes = new byte[] {3, 15};
    int[] columnMetadata = new int[] {0, 20};
    BitSet nullability = new BitSet();
    nullability.set(1);
    BitSet signedness = new BitSet();
    signedness.set(0);
    String[] enumValues = new String[] {"NEW", "DONE"};
    LinkedHashMap<Integer, Integer> prefixedPrimaryKeys = new LinkedHashMap<>();
    prefixedPrimaryKeys.put(1, 16);

    when(tableMap.getTableId()).thenReturn(7L);
    when(tableMap.getDatabase()).thenReturn("appdb");
    when(tableMap.getTable()).thenReturn("widgets");
    when(tableMap.getColumnTypes()).thenReturn(columnTypes);
    when(tableMap.getColumnMetadata()).thenReturn(columnMetadata);
    when(tableMap.getColumnNullability()).thenReturn(nullability);
    when(tableMap.getEventMetadata()).thenReturn(metadata);
    when(metadata.getColumnNames()).thenReturn(List.of("id", "status"));
    when(metadata.getSimplePrimaryKeys()).thenReturn(List.of(0));
    when(metadata.getPrimaryKeysWithPrefix()).thenReturn(prefixedPrimaryKeys);
    when(metadata.getSignedness()).thenReturn(signedness);
    when(metadata.getEnumStrValues()).thenReturn(List.<String[]>of(enumValues));

    BinaryLogClient client = mock(BinaryLogClient.class);
    when(client.getBinlogFilename()).thenReturn("mysql-bin.000321");
    AtomicReference<String> currentFilename = new AtomicReference<>("");
    AtomicReference<GtidSet> currentGtidSet =
        new AtomicReference<>(new GtidSet("24bc785e-9d1b-11ee-b9d1-0242ac120002:1-9"));
    AtomicReference<MySqlSourcePosition> currentPosition = new AtomicReference<>();

    MySqlBinlogMessage.TableMap translated =
        (MySqlBinlogMessage.TableMap)
            translate(
                tableMap,
                header(456L, TX_TIME),
                client,
                currentFilename,
                currentGtidSet,
                currentPosition);

    columnTypes[0] = 99;
    columnMetadata[1] = 999;
    nullability.clear(1);
    signedness.clear(0);
    enumValues[0] = "BROKEN";

    assertThat(currentFilename.get()).isEqualTo("mysql-bin.000321");
    assertThat(translated.tableId()).isEqualTo(7L);
    assertThat(translated.databaseName()).isEqualTo("appdb");
    assertThat(translated.tableName()).isEqualTo("widgets");
    assertThat(translated.position())
        .isEqualTo(
            new MySqlSourcePosition(
                "mysql-bin.000321",
                456L,
                "24bc785e-9d1b-11ee-b9d1-0242ac120002:1-9"));
    assertThat(translated.eventTimestamp()).isEqualTo(TX_TIME);
    assertThat(currentPosition.get()).isEqualTo(translated.position());
    assertThat(translated.columnTypes()).containsExactly((byte) 3, (byte) 15);
    assertThat(translated.columnMetadata()).containsExactly(0, 20);
    assertThat(translated.columnNullability()).isEqualTo(bitSetOf(1));
    assertThat(translated.columnNames()).containsExactly("id", "status");
    assertThat(translated.primaryKeyColumnIndexes()).containsExactly(0, 1);
    assertThat(translated.signedness()).isEqualTo(bitSetOf(0));
    assertThat(translated.enumValues()).hasSize(1);
    assertThat(translated.enumValues().get(0)).containsExactly("NEW", "DONE");
  }

  @Test
  void failsClosedWhenFilenameIsUnknownBeforeANonRotateEvent() throws Exception {
    QueryEventData query = mock(QueryEventData.class);
    when(query.getDatabase()).thenReturn("appdb");
    when(query.getSql()).thenReturn("BEGIN");

    BinaryLogClient client = mock(BinaryLogClient.class);
    when(client.getBinlogFilename()).thenReturn(null);

    assertThatThrownBy(
            () ->
                translate(
                    query,
                    header(456L, TX_TIME),
                    client,
                    new AtomicReference<>(null),
                    new AtomicReference<>(null),
                    new AtomicReference<>(null)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("before the current binlog filename was known");
  }

  @Test
  void accumulatesMySqlGtidSetAcrossEvents() throws Exception {
    BinaryLogClient client = mock(BinaryLogClient.class);
    when(client.getBinlogFilename()).thenReturn("mysql-bin.000001");
    AtomicReference<String> currentFilename = new AtomicReference<>("mysql-bin.000001");
    AtomicReference<GtidSet> currentGtidSet = new AtomicReference<>();
    AtomicReference<MySqlSourcePosition> currentPosition = new AtomicReference<>();

    GtidEventData gtidEvent = mock(GtidEventData.class);
    when(gtidEvent.getMySqlGtid())
        .thenReturn(MySqlGtid.fromString("24BC785E-9D1B-11EE-B9D1-0242AC120002:1"));

    QueryEventData query = mock(QueryEventData.class);
    when(query.getDatabase()).thenReturn("appdb");
    when(query.getSql()).thenReturn("BEGIN");

    MySqlBinlogMessage.Gtid gtidMessage =
        (MySqlBinlogMessage.Gtid)
            translate(
                gtidEvent,
                header(222L, TX_TIME),
                client,
                currentFilename,
                currentGtidSet,
                currentPosition);
    MySqlBinlogMessage.Query queryMessage =
        (MySqlBinlogMessage.Query)
            translate(
                query,
                header(333L, TX_TIME.plusSeconds(1)),
                client,
                currentFilename,
                currentGtidSet,
                currentPosition);

    assertThat(gtidMessage.gtid())
        .isEqualTo("24bc785e-9d1b-11ee-b9d1-0242ac120002:1");
    assertThat(currentGtidSet.get()).isNotNull();
    assertThat(currentGtidSet.get().toString())
        .isEqualTo("24bc785e-9d1b-11ee-b9d1-0242ac120002:1-1");
    assertThat(queryMessage.position())
        .isEqualTo(
            new MySqlSourcePosition(
                "mysql-bin.000001",
                333L,
                "24bc785e-9d1b-11ee-b9d1-0242ac120002:1-1"));
    assertThat(currentPosition.get()).isEqualTo(queryMessage.position());
  }

  @Test
  void configuresGtidResumeOnlyWhenExplicitlyRequested() {
    BinaryLogClient client = mock(BinaryLogClient.class);
    MySqlSourcePosition startPosition =
        new MySqlSourcePosition("mysql-bin.000123", 456789L, "uuid:1-42");

    JdbcMySqlBinlogStreamFactory.configureStartPosition(
        client,
        new MySqlBinlogStreamRequest(
            "127.0.0.1",
            3306,
            "appdb",
            "dblog",
            "dblog",
            223344L,
            startPosition,
            false,
            Duration.ofSeconds(5),
            Duration.ofSeconds(5),
            Duration.ofSeconds(300),
            Duration.ofMinutes(10),
            50_000));

    verify(client).setBinlogFilename("mysql-bin.000123");
    verify(client).setBinlogPosition(456789L);
    verify(client).setGtidSet("uuid:1-42");
    verify(client).setUseBinlogFilenamePositionInGtidMode(true);

    BinaryLogClient fallbackClient = mock(BinaryLogClient.class);
    JdbcMySqlBinlogStreamFactory.configureStartPosition(
        fallbackClient,
        new MySqlBinlogStreamRequest(
            "127.0.0.1",
            3306,
            "appdb",
            "dblog",
            "dblog",
            223344L,
            startPosition,
            true,
            Duration.ofSeconds(5),
            Duration.ofSeconds(5),
            Duration.ofSeconds(300),
            Duration.ofMinutes(10),
            50_000));

    verify(fallbackClient).setBinlogFilename("mysql-bin.000123");
    verify(fallbackClient).setBinlogPosition(456789L);
    verify(fallbackClient).setGtidSet("uuid:1-42");
    verify(fallbackClient).setUseBinlogFilenamePositionInGtidMode(false);
  }

  private static MySqlBinlogMessage translate(
      EventData data,
      EventHeaderV4 header,
      BinaryLogClient client,
      AtomicReference<String> currentFilename,
      AtomicReference<GtidSet> currentGtidSet,
      AtomicReference<MySqlSourcePosition> currentPosition)
      throws Exception {
    Event event = mock(Event.class);
    when(event.getData()).thenReturn(data);
    when(event.getHeader()).thenReturn(header);
    Method method =
        JdbcMySqlBinlogStreamFactory.class.getDeclaredMethod(
            "translate",
            Event.class,
            BinaryLogClient.class,
            AtomicReference.class,
            AtomicReference.class,
            AtomicReference.class);
    method.setAccessible(true);
    try {
      return (MySqlBinlogMessage)
          method.invoke(null, event, client, currentFilename, currentGtidSet, currentPosition);
    } catch (InvocationTargetException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof Exception exception) {
        throw exception;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw ex;
    }
  }

  private static EventHeaderV4 header(long nextPosition, Instant timestamp) {
    EventHeaderV4 header = mock(EventHeaderV4.class);
    when(header.getNextPosition()).thenReturn(nextPosition);
    when(header.getTimestamp()).thenReturn(timestamp.toEpochMilli());
    return header;
  }

  private static BitSet bitSetOf(int... indexes) {
    BitSet bitSet = new BitSet();
    for (int index : indexes) {
      bitSet.set(index);
    }
    return bitSet;
  }
}
