package io.github.aandreakis.dblog.state.h2;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.ref.Cleaner;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Shared Hikari pool registry for local H2-backed runtime state stores. */
public final class HikariLeaseRegistry {
  private static final Cleaner CLEANER = Cleaner.create();
  private static final ConcurrentMap<PoolConfigKey, SharedPool> POOLS = new ConcurrentHashMap<>();

  private HikariLeaseRegistry() {}

  public static Lease acquire(
      String driverClassName, String jdbcUrl, String poolLabel, int maximumPoolSize) {
    return acquire(driverClassName, jdbcUrl, null, null, poolLabel, maximumPoolSize, 0);
  }

  public static Lease acquire(
      String driverClassName,
      String jdbcUrl,
      String poolLabel,
      int maximumPoolSize,
      int minimumIdle) {
    return acquire(
        driverClassName,
        jdbcUrl,
        null,
        null,
        poolLabel,
        maximumPoolSize,
        minimumIdle,
        2_000L);
  }

  public static Lease acquire(
      String driverClassName,
      String jdbcUrl,
      String username,
      String password,
      String poolLabel,
      int maximumPoolSize) {
    return acquire(
        driverClassName,
        jdbcUrl,
        username,
        password,
        poolLabel,
        maximumPoolSize,
        0,
        2_000L);
  }

  public static Lease acquire(
      String driverClassName,
      String jdbcUrl,
      String username,
      String password,
      String poolLabel,
      int maximumPoolSize,
      int minimumIdle) {
    return acquire(
        driverClassName,
        jdbcUrl,
        username,
        password,
        poolLabel,
        maximumPoolSize,
        minimumIdle,
        2_000L);
  }

  public static Lease acquire(
      String driverClassName,
      String jdbcUrl,
      String username,
      String password,
      String poolLabel,
      int maximumPoolSize,
      int minimumIdle,
      long connectionTimeoutMillis) {
    String requiredDriverClassName = requireNonBlank(driverClassName, "driverClassName");
    String requiredJdbcUrl = requireNonBlank(jdbcUrl, "jdbcUrl");
    String requiredPoolLabel = requireNonBlank(poolLabel, "poolLabel");
    if (maximumPoolSize <= 0) {
      throw new IllegalArgumentException("maximumPoolSize must be > 0");
    }
    if (minimumIdle < 0) {
      throw new IllegalArgumentException("minimumIdle must be >= 0");
    }
    if (minimumIdle > maximumPoolSize) {
      throw new IllegalArgumentException("minimumIdle must be <= maximumPoolSize");
    }
    if (connectionTimeoutMillis <= 0L) {
      throw new IllegalArgumentException("connectionTimeoutMillis must be > 0");
    }
    PoolConfigKey key =
        new PoolConfigKey(
            requiredDriverClassName,
            requiredJdbcUrl,
            username,
            passwordFingerprint(password),
            requiredPoolLabel,
            maximumPoolSize,
            minimumIdle,
            connectionTimeoutMillis);
    SharedPool sharedPool =
        POOLS.compute(
            key,
            (ignored, existing) -> {
              if (existing != null) {
                existing.references().incrementAndGet();
                return existing;
              }
              HikariConfig config = new HikariConfig();
              config.setPoolName(poolName(key));
              config.setDriverClassName(requiredDriverClassName);
              config.setJdbcUrl(requiredJdbcUrl);
              if (username != null) {
                config.setUsername(username);
              }
              if (password != null) {
                config.setPassword(password);
              }
              config.setMaximumPoolSize(maximumPoolSize);
              config.setMinimumIdle(minimumIdle);
              config.setConnectionTimeout(connectionTimeoutMillis);
              config.setValidationTimeout(Math.max(1_000L, Math.min(connectionTimeoutMillis, 5_000L)));
              config.setInitializationFailTimeout(1L);
              config.setRegisterMbeans(false);
              return new SharedPool(new HikariDataSource(config), new AtomicInteger(1));
            });
    return new Lease(key, sharedPool);
  }

  private static void release(PoolConfigKey key, SharedPool sharedPool) {
    AtomicBoolean shouldClose = new AtomicBoolean(false);
    POOLS.compute(
        key,
        (ignored, existing) -> {
          if (existing != sharedPool) {
            return existing;
          }
          if (sharedPool.references().decrementAndGet() > 0) {
            return sharedPool;
          }
          shouldClose.set(true);
          return null;
        });
    if (shouldClose.get()) {
      closeDataSource(sharedPool.dataSource());
    }
  }

  private static void closeDataSource(HikariDataSource dataSource) {
    boolean interrupted = Thread.interrupted();
    try {
      dataSource.close();
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static String poolName(PoolConfigKey key) {
    String normalizedLabel = key.poolLabel().replaceAll("[^A-Za-z0-9]+", "-");
    return "dblog-" + normalizedLabel + "-" + Integer.toUnsignedString(key.hashCode(), 16);
  }

  private static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static String passwordFingerprint(String password) {
    if (password == null) {
      return "<null>";
    }
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(digest.digest(password.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 should always exist", e);
    }
  }

  static int activePoolCountForTests() {
    return POOLS.size();
  }

  static void resetForTests() {
    List<SharedPool> pools = new ArrayList<>(POOLS.values());
    POOLS.clear();
    for (SharedPool pool : pools) {
      closeDataSource(pool.dataSource());
    }
  }

  static record PoolConfigKey(
      String driverClassName,
      String jdbcUrl,
      String username,
      String passwordFingerprint,
      String poolLabel,
      int maximumPoolSize,
      int minimumIdle,
      long connectionTimeoutMillis) {
    PoolConfigKey {
      driverClassName = requireNonBlank(driverClassName, "driverClassName");
      jdbcUrl = requireNonBlank(jdbcUrl, "jdbcUrl");
      poolLabel = requireNonBlank(poolLabel, "poolLabel");
      if (maximumPoolSize <= 0) {
        throw new IllegalArgumentException("maximumPoolSize must be > 0");
      }
      if (minimumIdle < 0) {
        throw new IllegalArgumentException("minimumIdle must be >= 0");
      }
      if (minimumIdle > maximumPoolSize) {
        throw new IllegalArgumentException("minimumIdle must be <= maximumPoolSize");
      }
      if (connectionTimeoutMillis <= 0L) {
        throw new IllegalArgumentException("connectionTimeoutMillis must be > 0");
      }
    }
  }

  static record SharedPool(HikariDataSource dataSource, AtomicInteger references) {
    SharedPool {
      Objects.requireNonNull(dataSource, "dataSource");
      Objects.requireNonNull(references, "references");
    }
  }

  public static final class Lease implements AutoCloseable {
    private final PoolConfigKey key;
    private final SharedPool sharedPool;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Cleaner.Cleanable cleanable;

    private Lease(PoolConfigKey key, SharedPool sharedPool) {
      this.key = Objects.requireNonNull(key, "key");
      this.sharedPool = Objects.requireNonNull(sharedPool, "sharedPool");
      this.cleanable = CLEANER.register(this, () -> release(key, sharedPool));
    }

    public Connection openConnection() throws SQLException {
      return sharedPool.dataSource().getConnection();
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      cleanable.clean();
    }
  }
}
