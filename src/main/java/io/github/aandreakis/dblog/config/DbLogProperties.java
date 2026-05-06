package io.github.aandreakis.dblog.config;

import io.github.aandreakis.dblog.config.validation.ValidRuntimeConfiguration;
import io.github.aandreakis.dblog.config.validation.ValidScenarioConfiguration;
import io.github.aandreakis.dblog.config.validation.ValidTargetConfiguration;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** Typed Spring configuration surface for DBLog runtime, sinks, and scenario runs. */
@Validated
@ValidRuntimeConfiguration
@ValidScenarioConfiguration
@ValidTargetConfiguration
@ConfigurationProperties(prefix = "dblog")
public class DbLogProperties {
  private BootMode bootMode;
  @Valid private final Chunk chunk = new Chunk();
  @Valid private final Checkpoint checkpoint = new Checkpoint();
  @Valid private final Heartbeat heartbeat = new Heartbeat();
  @Valid private final Sink sink = new Sink();
  @Valid private final DbLogTargetProperties target = new DbLogTargetProperties();
  @Valid private final Observability observability = new Observability();
  @Valid private final DbLogControlPlaneProperties controlPlane = new DbLogControlPlaneProperties();
  @Valid private final Runtime runtime = new Runtime();
  @Valid private final Source source = new Source();
  @Valid private final DbLogScenarioProperties scenario = new DbLogScenarioProperties();

  public BootMode getBootMode() {
    return bootMode;
  }

  public void setBootMode(BootMode bootMode) {
    this.bootMode = bootMode;
  }

  public Chunk getChunk() {
    return chunk;
  }

  public Checkpoint getCheckpoint() {
    return checkpoint;
  }

  public Heartbeat getHeartbeat() {
    return heartbeat;
  }

  public Sink getSink() {
    return sink;
  }

  public DbLogTargetProperties getTarget() {
    return target;
  }

  public Observability getObservability() {
    return observability;
  }

  public DbLogControlPlaneProperties getControlPlane() {
    return controlPlane;
  }

  public Runtime getRuntime() {
    return runtime;
  }

  public Source getSource() {
    return source;
  }

  public DbLogScenarioProperties getScenario() {
    return scenario;
  }

  public enum BootMode {
    RUNTIME,
    SCENARIO,
    STARTUP_CHECK
  }

  public enum TargetDialect {
    POSTGRES,
    MYSQL
  }

  public static final class Chunk {
    /**
     * Per-chunk row cap. Upper bound is a reference-impl safety ceiling: chunks are materialized
     * fully in memory before reconcile, and a misconfigured multi-million-row chunk would OOM the
     * process before emitting the first row. Operators with larger throughput requirements should
     * leave this at its default (100) and submit more frequent chunks rather than raising the
     * ceiling. See docs/OPERATION.md §2.4.
     */
    @Min(1)
    @Max(100_000)
    private int size = 100;

    public int getSize() {
      return size;
    }

    public void setSize(int size) {
      this.size = size;
    }
  }

  public static final class Checkpoint {
    @Min(1)
    private int maxEvents = 100;

    @NotNull
    private Duration maxInterval = Duration.ofSeconds(5);

    public int getMaxEvents() {
      return maxEvents;
    }

    public void setMaxEvents(int maxEvents) {
      this.maxEvents = maxEvents;
    }

    public Duration getMaxInterval() {
      return maxInterval;
    }

    public void setMaxInterval(Duration maxInterval) {
      this.maxInterval = maxInterval;
    }
  }

  public static final class Heartbeat {
    @NotNull
    private Duration interval = Duration.ofSeconds(5);

    public Duration getInterval() {
      return interval;
    }

    public void setInterval(Duration interval) {
      this.interval = interval;
    }
  }

  public static final class Sink {
    @Valid private final Ndjson ndjson = new Ndjson();
    @Valid private final TypedH2 typedH2 = new TypedH2();
    @Valid private final Noop noop = new Noop();

    public Ndjson getNdjson() {
      return ndjson;
    }

    public TypedH2 getTypedH2() {
      return typedH2;
    }

    public Noop getNoop() {
      return noop;
    }
  }

  public static final class Ndjson {
    private boolean stdout = false;
    private Path path;

    public boolean isStdout() {
      return stdout;
    }

    public void setStdout(boolean stdout) {
      this.stdout = stdout;
    }

    public Path getPath() {
      return path;
    }

    public void setPath(Path path) {
      this.path = path;
    }
  }

  public static final class TypedH2 {
    private Path path;

    public Path getPath() {
      return path;
    }

    public void setPath(Path path) {
      this.path = path;
    }
  }

  public static final class Noop {
    private boolean enabled = false;

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }
  }

  public static final class Observability {
    private boolean metricsEnabled = true;

    public boolean isMetricsEnabled() {
      return metricsEnabled;
    }

    public void setMetricsEnabled(boolean metricsEnabled) {
      this.metricsEnabled = metricsEnabled;
    }
  }

  public static final class Runtime {
    /**
     * H2 database-file <strong>prefix</strong>, not a directory. H2 writes
     * {@code <state-path>.mv.db} (data) and {@code <state-path>.trace.db} (trace) next to this
     * location. To reset state between runs, remove {@code <state-path>*}, not a directory at
     * {@code <state-path>/}.
     */
    private Path statePath;
    private boolean retainTransactionHistory = false;

    public Path getStatePath() {
      return statePath;
    }

    public void setStatePath(Path statePath) {
      this.statePath = statePath;
    }

    public boolean isRetainTransactionHistory() {
      return retainTransactionHistory;
    }

    public void setRetainTransactionHistory(boolean retainTransactionHistory) {
      this.retainTransactionHistory = retainTransactionHistory;
    }
  }

  public static final class Source {
    private String adapter;
    private String id;
    private List<String> tables = new ArrayList<>();

    @Valid private final DbLogMysqlProperties mysql = new DbLogMysqlProperties();
    @Valid private final DbLogPostgresProperties postgres = new DbLogPostgresProperties();

    public String getAdapter() {
      return adapter;
    }

    public void setAdapter(String adapter) {
      this.adapter = adapter;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public List<String> getTables() {
      return tables;
    }

    public void setTables(List<String> tables) {
      this.tables = tables == null ? new ArrayList<>() : new ArrayList<>(tables);
    }

    public DbLogMysqlProperties getMysql() {
      return mysql;
    }

    public DbLogPostgresProperties getPostgres() {
      return postgres;
    }
  }
}
