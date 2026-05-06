package io.github.aandreakis.dblog.state.h2;

import io.github.aandreakis.dblog.state.api.DumpProgressRepository;
import io.github.aandreakis.dblog.state.api.DumpRequestRepository;
import io.github.aandreakis.dblog.state.api.RuntimeStateStore;
import io.github.aandreakis.dblog.state.api.SchemaStateRepository;
import io.github.aandreakis.dblog.state.api.SourceOwnershipRepository;
import io.github.aandreakis.dblog.state.api.StreamPositionRepository;
import io.github.aandreakis.dblog.state.jdbc.JdbcDumpProgressRepository;
import io.github.aandreakis.dblog.state.jdbc.JdbcDumpRequestRepository;
import io.github.aandreakis.dblog.state.jdbc.JdbcRuntimeStateStore;
import io.github.aandreakis.dblog.state.jdbc.JdbcSchemaStateRepository;
import io.github.aandreakis.dblog.state.jdbc.JdbcSourceOwnershipRepository;
import io.github.aandreakis.dblog.state.jdbc.JdbcStateStoreSchemaBootstrap;
import io.github.aandreakis.dblog.state.jdbc.JdbcStateStoreSupport;
import io.github.aandreakis.dblog.state.jdbc.JdbcStreamPositionRepository;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class H2RuntimeStateStore implements RuntimeStateStore, AutoCloseable {
  private static final String DRIVER_CLASS_NAME = "org.h2.Driver";
  private static final int MAXIMUM_POOL_SIZE = 16;
  private static final int MINIMUM_IDLE = 0;
  private static final long CONNECTION_TIMEOUT_MILLIS = 10_000L;
  private static final ConcurrentMap<String, Object> BOOTSTRAP_LOCKS = new ConcurrentHashMap<>();

  private final JdbcStateStoreSupport jdbc;
  private final JdbcRuntimeStateStore delegate;

  public H2RuntimeStateStore(Path databasePath) {
    Objects.requireNonNull(databasePath, "databasePath");
    String jdbcUrl = h2JdbcUrl(databasePath);
    HikariLeaseRegistry.Lease lease =
        HikariLeaseRegistry.acquire(
            DRIVER_CLASS_NAME,
            jdbcUrl,
            null,
            null,
            "h2-runtime-state-store",
            MAXIMUM_POOL_SIZE,
            MINIMUM_IDLE,
            CONNECTION_TIMEOUT_MILLIS);
    JdbcStateStoreSupport acquiredJdbc =
        new JdbcStateStoreSupport(DRIVER_CLASS_NAME, lease::openConnection, lease);
    try {
      synchronized (bootstrapLock(jdbcUrl)) {
        acquiredJdbc.withTransaction(
            connection -> {
              JdbcStateStoreSchemaBootstrap.initialize(connection);
              return null;
            });
      }
      this.delegate =
          new JdbcRuntimeStateStore(
              acquiredJdbc,
              new JdbcStreamPositionRepository(acquiredJdbc),
              new JdbcDumpRequestRepository(acquiredJdbc),
              new JdbcDumpProgressRepository(acquiredJdbc),
              new JdbcSchemaStateRepository(acquiredJdbc),
              new JdbcSourceOwnershipRepository(acquiredJdbc));
    } catch (RuntimeException bootstrapFailure) {
      // Constructor is aborting; the caller will never receive an instance and will never call
      // close(), so we must release the Hikari lease here or leak a pool in the registry until
      // JVM exit.
      try {
        acquiredJdbc.close();
      } catch (RuntimeException releaseFailure) {
        bootstrapFailure.addSuppressed(releaseFailure);
      }
      throw bootstrapFailure;
    }
    this.jdbc = acquiredJdbc;
  }

  private static Object bootstrapLock(String jdbcUrl) {
    return BOOTSTRAP_LOCKS.computeIfAbsent(jdbcUrl, ignored -> new Object());
  }

  public static boolean isDriverAvailable() {
    try {
      Class.forName(DRIVER_CLASS_NAME);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  public static String h2JdbcUrl(Path databasePath) {
    Objects.requireNonNull(databasePath, "databasePath");
    String normalized = databasePath.toAbsolutePath().normalize().toString().replace('\\', '/');
    // WRITE_DELAY=0 forces H2 to flush each committed transaction to disk immediately rather
    // than holding it in its default 500ms write-behind buffer. This aligns with SPEC §14's
    // "restart-safe via local checkpoint state" claim: without it, a kill -9 between commit
    // return and flush can silently lose up to half a second of state.
    return "jdbc:h2:file:" + normalized + ";DB_CLOSE_ON_EXIT=FALSE;WRITE_DELAY=0";
  }

  @Override
  public StreamPositionRepository streamPositions() {
    return delegate.streamPositions();
  }

  @Override
  public DumpRequestRepository dumpRequests() {
    return delegate.dumpRequests();
  }

  @Override
  public DumpProgressRepository dumpProgress() {
    return delegate.dumpProgress();
  }

  @Override
  public SchemaStateRepository schemas() {
    return delegate.schemas();
  }

  @Override
  public SourceOwnershipRepository ownership() {
    return delegate.ownership();
  }

  @Override
  public void invalidateRuntimeStateForPkChange(String sourceId, String reason) {
    delegate.invalidateRuntimeStateForPkChange(sourceId, reason);
  }

  @Override
  public void close() {
    jdbc.close();
  }
}
