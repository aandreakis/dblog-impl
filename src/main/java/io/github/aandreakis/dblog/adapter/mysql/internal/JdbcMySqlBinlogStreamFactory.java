package io.github.aandreakis.dblog.adapter.mysql.internal;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.MySqlGtid;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import io.github.aandreakis.dblog.adapter.mysql.MySqlSourcePosition;
import io.github.aandreakis.dblog.runtime.sql.BoundedSourceEventQueue;
import io.github.aandreakis.dblog.runtime.sql.SourceFlowControlSnapshot;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thin MySQL binlog stream wrapper built directly on mysql-binlog-connector-java.
 *
 * <p>Third-party event classes are translated into the smaller DBLog-owned {@link
 * MySqlBinlogMessage} hierarchy before the rest of the adapter sees them.
 */
public final class JdbcMySqlBinlogStreamFactory implements MySqlBinlogStreamFactory {
  @Override
  public MySqlBinlogStream open(MySqlBinlogStreamRequest request) throws SQLException {
    Objects.requireNonNull(request, "request");
    BinaryLogClient client =
        new BinaryLogClient(
            request.hostname(),
            request.port(),
            request.databaseName(),
            request.username(),
            request.password());
    EventDeserializer eventDeserializer = new EventDeserializer();
    eventDeserializer.setCompatibilityMode(
        EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
    client.setEventDeserializer(eventDeserializer);
    configureClientConnectionOptions(client, request);
    configureStartPosition(client, request);

    BoundedSourceEventQueue<MySqlBinlogMessage> queue =
        new BoundedSourceEventQueue<>("mysql", request.sourceEventQueueCapacity());
    AtomicReference<SQLException> failure = new AtomicReference<>();
    AtomicBoolean closedByCaller = new AtomicBoolean();
    AtomicReference<String> currentBinlogFilename =
        new AtomicReference<>(
            request.startPosition() == null ? null : request.startPosition().binlogFilename());
    AtomicReference<GtidSet> currentGtidSet =
        new AtomicReference<>(initialGtidSet(request.startPosition()));
    AtomicReference<MySqlSourcePosition> currentPosition =
        new AtomicReference<>(request.startPosition());

    client.registerLifecycleListener(
        new BinaryLogClient.AbstractLifecycleListener() {
          @Override
          public void onCommunicationFailure(BinaryLogClient ignored, Exception ex) {
            SQLException wrapped =
                new SQLException("MySQL binlog stream communication failure", ex);
            if (failure.compareAndSet(null, wrapped)) {
              queue.markFailed(wrapped);
            }
          }

          @Override
          public void onEventDeserializationFailure(BinaryLogClient ignored, Exception ex) {
            SQLException wrapped =
                new SQLException("MySQL binlog stream event deserialization failure", ex);
            if (failure.compareAndSet(null, wrapped)) {
              queue.markFailed(wrapped);
            }
          }

          @Override
          public void onDisconnect(BinaryLogClient ignored) {
            if (!closedByCaller.get()) {
              SQLTransientConnectionException wrapped =
                  new SQLTransientConnectionException(
                      "MySQL binlog stream disconnected unexpectedly");
              if (failure.compareAndSet(null, wrapped)) {
                queue.markFailed(wrapped);
              }
            }
          }
        });

    client.registerEventListener(
        event -> {
          try {
            MySqlBinlogMessage translated =
                translate(event, client, currentBinlogFilename, currentGtidSet, currentPosition);
            if (translated != null) {
              queue.enqueue(translated);
            }
          } catch (RuntimeException ex) {
            SQLException wrapped =
                new SQLException(
                    "Failed to translate MySQL binlog event into the message shape",
                    ex);
            if (failure.compareAndSet(null, wrapped)) {
              queue.markFailed(wrapped);
            }
            try {
              closedByCaller.set(true);
              client.disconnect();
            } catch (IOException disconnectEx) {
              failure.get().addSuppressed(disconnectEx);
            }
          }
        });

    try {
      long timeoutMillis = request.connectTimeout() == null ? 3000L : request.connectTimeout().toMillis();
      client.connect(timeoutMillis);
      currentBinlogFilename.set(client.getBinlogFilename());
      currentPosition.set(
          new MySqlSourcePosition(
              currentBinlogFilename.get(),
              client.getBinlogPosition(),
              gtidStateValue(currentGtidSet.get())));
      return new QueueingMySqlBinlogStream(
          client, queue, failure, closedByCaller, currentPosition);
    } catch (IOException | TimeoutException ex) {
      try {
        closedByCaller.set(true);
        client.disconnect();
      } catch (IOException disconnectEx) {
        ex.addSuppressed(disconnectEx);
      }
      throw new SQLException("Failed to open MySQL binlog stream", ex);
    }
  }

  static void configureStartPosition(BinaryLogClient client, MySqlBinlogStreamRequest request) {
    Objects.requireNonNull(client, "client");
    Objects.requireNonNull(request, "request");
    if (request.startPosition() == null) {
      return;
    }
    client.setBinlogFilename(request.startPosition().binlogFilename());
    client.setBinlogPosition(request.startPosition().binlogPosition());
    if (request.startPosition().gtidSet() != null) {
      client.setGtidSet(request.startPosition().gtidSet());
      client.setUseBinlogFilenamePositionInGtidMode(!request.useGtidResume());
    }
  }

  static void configureClientConnectionOptions(
      BinaryLogClient client, MySqlBinlogStreamRequest request) {
    Objects.requireNonNull(client, "client");
    Objects.requireNonNull(request, "request");
    client.setServerId(request.serverId());
    client.setKeepAlive(true);
    if (request.connectTimeout() != null) {
      client.setConnectTimeout(request.connectTimeout().toMillis());
    }
    if (request.heartbeatInterval() != null) {
      client.setHeartbeatInterval(Math.max(1L, request.heartbeatInterval().toMillis()));
    }
    if (request.keepAliveInterval() != null) {
      client.setKeepAliveInterval(Math.max(1L, request.keepAliveInterval().toMillis()));
    }
    if (request.netWriteTimeout() != null) {
      client.setNetWriteTimeout(Math.max(1L, request.netWriteTimeout().toSeconds()));
    }
  }

  static MySqlBinlogMessage translate(
      Event event,
      BinaryLogClient client,
      AtomicReference<String> currentBinlogFilename,
      AtomicReference<GtidSet> currentGtidSet,
      AtomicReference<MySqlSourcePosition> currentPosition) {
    Objects.requireNonNull(event, "event");
    Objects.requireNonNull(client, "client");
    Objects.requireNonNull(currentBinlogFilename, "currentBinlogFilename");
    Objects.requireNonNull(currentGtidSet, "currentGtidSet");

    EventData data = event.getData();
    if (data == null) {
      return null;
    }
    EventHeaderV4 header = (EventHeaderV4) event.getHeader();
    Instant eventTimestamp = Instant.ofEpochMilli(header.getTimestamp());

    if (data instanceof RotateEventData rotateEventData) {
      currentBinlogFilename.set(rotateEventData.getBinlogFilename());
      MySqlSourcePosition position =
          new MySqlSourcePosition(
              rotateEventData.getBinlogFilename(),
              rotateEventData.getBinlogPosition(),
              gtidStateValue(currentGtidSet.get()));
      return new MySqlBinlogMessage.Rotate(
          rotateEventData.getBinlogFilename(),
          rotateEventData.getBinlogPosition(),
          position,
          eventTimestamp);
    }

    String binlogFilename = currentBinlogFilename.get();
    if (binlogFilename == null || binlogFilename.isBlank()) {
      binlogFilename = client.getBinlogFilename();
      currentBinlogFilename.set(binlogFilename);
    }
    if (binlogFilename == null || binlogFilename.isBlank()) {
      throw new IllegalStateException(
          "MySQL binlog event arrived before the current binlog filename was known");
    }

    MySqlSourcePosition position =
        new MySqlSourcePosition(
            binlogFilename, header.getNextPosition(), gtidStateValue(currentGtidSet.get()));
    currentPosition.set(position);
    if (data instanceof GtidEventData gtidEventData) {
      updateGtidSet(gtidString(gtidEventData), currentGtidSet);
      return new MySqlBinlogMessage.Gtid(gtidString(gtidEventData), position, eventTimestamp);
    }
    if (data instanceof QueryEventData queryEventData) {
      return new MySqlBinlogMessage.Query(
          queryEventData.getDatabase(), queryEventData.getSql(), position, eventTimestamp);
    }
    if (data instanceof TableMapEventData tableMapEventData) {
      TableMapEventMetadata metadata = tableMapEventData.getEventMetadata();
      return new MySqlBinlogMessage.TableMap(
          tableMapEventData.getTableId(),
          tableMapEventData.getDatabase(),
          tableMapEventData.getTable(),
          tableMapEventData.getColumnTypes(),
          tableMapEventData.getColumnMetadata(),
          tableMapEventData.getColumnNullability(),
          metadata == null ? List.of() : copyStrings(metadata.getColumnNames()),
          primaryKeyColumnIndexes(metadata),
          metadata == null ? null : metadata.getSignedness(),
          metadata == null ? List.of() : copyEnumValues(metadata.getEnumStrValues()),
          position,
          eventTimestamp);
    }
    if (data instanceof WriteRowsEventData writeRowsEventData) {
      return MySqlBinlogMessage.trustedWriteRows(
          writeRowsEventData.getTableId(),
          rowViews(writeRowsEventData.getRows()),
          position,
          eventTimestamp);
    }
    if (data instanceof UpdateRowsEventData updateRowsEventData) {
      return MySqlBinlogMessage.trustedUpdateRows(
          updateRowsEventData.getTableId(),
          rowChanges(updateRowsEventData.getRows()),
          position,
          eventTimestamp);
    }
    if (data instanceof DeleteRowsEventData deleteRowsEventData) {
      return MySqlBinlogMessage.trustedDeleteRows(
          deleteRowsEventData.getTableId(),
          rowViews(deleteRowsEventData.getRows()),
          position,
          eventTimestamp);
    }
    if (data instanceof XidEventData xidEventData) {
      return new MySqlBinlogMessage.Commit(
          Long.toString(xidEventData.getXid()), position, eventTimestamp);
    }
    return null;
  }

  private static void updateGtidSet(String observedGtid, AtomicReference<GtidSet> currentGtidSet) {
    if (observedGtid == null) {
      return;
    }
    GtidSet existing = currentGtidSet.get();
    if (existing == null) {
      currentGtidSet.set(new GtidSet(observedGtid));
      return;
    }
    existing.add(observedGtid);
  }

  private static GtidSet initialGtidSet(MySqlSourcePosition startPosition) {
    if (startPosition == null || startPosition.gtidSet() == null) {
      return null;
    }
    return new GtidSet(startPosition.gtidSet());
  }

  private static String gtidStateValue(GtidSet currentGtidSet) {
    return currentGtidSet == null ? null : currentGtidSet.toString();
  }

  private static List<Object[]> rowViews(List<Serializable[]> rows) {
    List<Object[]> viewed = new ArrayList<>(rows.size());
    for (Serializable[] row : rows) {
      viewed.add(row);
    }
    return viewed;
  }

  private static List<MySqlBinlogMessage.RowChange> rowChanges(
      List<Map.Entry<Serializable[], Serializable[]>> rowChanges) {
    List<MySqlBinlogMessage.RowChange> copied = new ArrayList<>(rowChanges.size());
    for (Map.Entry<Serializable[], Serializable[]> rowChange : rowChanges) {
      copied.add(new MySqlBinlogMessage.RowChange(rowChange.getKey(), rowChange.getValue()));
    }
    return copied;
  }

  private static String gtidString(GtidEventData gtidEventData) {
    Objects.requireNonNull(gtidEventData, "gtidEventData");
    MySqlGtid mySqlGtid = gtidEventData.getMySqlGtid();
    return mySqlGtid == null ? null : mySqlGtid.toString();
  }

  private static List<String> copyStrings(List<String> values) {
    return values == null ? List.of() : List.copyOf(values);
  }

  private static List<String[]> copyEnumValues(List<String[]> values) {
    if (values == null) {
      return List.of();
    }
    List<String[]> copied = new ArrayList<>(values.size());
    for (String[] value : values) {
      copied.add(value == null ? null : value.clone());
    }
    return List.copyOf(copied);
  }

  private static List<Integer> primaryKeyColumnIndexes(TableMapEventMetadata metadata) {
    if (metadata == null) {
      return List.of();
    }
    LinkedHashSet<Integer> indexes = new LinkedHashSet<>();
    if (metadata.getSimplePrimaryKeys() != null) {
      indexes.addAll(metadata.getSimplePrimaryKeys());
    }
    if (metadata.getPrimaryKeysWithPrefix() != null) {
      indexes.addAll(metadata.getPrimaryKeysWithPrefix().keySet());
    }
    return List.copyOf(indexes);
  }

  private static final class QueueingMySqlBinlogStream implements MySqlBinlogStream {
    private final BinaryLogClient client;
    private final BoundedSourceEventQueue<MySqlBinlogMessage> queue;
    private final AtomicReference<SQLException> failure;
    private final AtomicBoolean closedByCaller;
    private final AtomicReference<MySqlSourcePosition> currentPosition;

    private QueueingMySqlBinlogStream(
        BinaryLogClient client,
        BoundedSourceEventQueue<MySqlBinlogMessage> queue,
        AtomicReference<SQLException> failure,
        AtomicBoolean closedByCaller,
        AtomicReference<MySqlSourcePosition> currentPosition) {
      this.client = Objects.requireNonNull(client, "client");
      this.queue = Objects.requireNonNull(queue, "queue");
      this.failure = Objects.requireNonNull(failure, "failure");
      this.closedByCaller = Objects.requireNonNull(closedByCaller, "closedByCaller");
      this.currentPosition = Objects.requireNonNull(currentPosition, "currentPosition");
    }

    @Override
    public Optional<MySqlBinlogMessage> readMessage() throws SQLException {
      SQLException streamFailure = failure.get();
      if (streamFailure != null && queue.depth() == 0) {
        throw streamFailure;
      }
      Optional<MySqlBinlogMessage> message = queue.pollNow();
      if (message.isPresent()) {
        return message;
      }
      streamFailure = failure.get();
      if (streamFailure != null && queue.depth() == 0) {
        throw streamFailure;
      }
      return Optional.empty();
    }

    @Override
    public void close() throws SQLException {
      try {
        closedByCaller.set(true);
        queue.close();
        client.disconnect();
      } catch (IOException ex) {
        throw new SQLException("Failed to close MySQL binlog stream", ex);
      }
    }

    @Override
    public SourceFlowControlSnapshot sourceFlowControlSnapshot() {
      return queue.snapshot();
    }

    @Override
    public Optional<MySqlSourcePosition> connectedPosition() {
      return Optional.ofNullable(currentPosition.get());
    }
  }
}
