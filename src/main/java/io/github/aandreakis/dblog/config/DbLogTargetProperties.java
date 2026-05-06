package io.github.aandreakis.dblog.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Target-apply configuration surface. */
public class DbLogTargetProperties {
  private boolean enabled = false;
  private DbLogProperties.TargetDialect dialect;
  private String jdbcUrl;
  private String username;
  private String password = "";
  @Valid private final List<DbLogTableMappingProperties> tableMappings = new ArrayList<>();

  @NotNull
  private Duration connectionTimeout = Duration.ofSeconds(2);

  @NotNull
  private Duration retryBackoff = Duration.ofSeconds(3);

  @Min(1)
  private int maximumPoolSize = 4;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public DbLogProperties.TargetDialect getDialect() {
    return dialect;
  }

  public void setDialect(DbLogProperties.TargetDialect dialect) {
    this.dialect = dialect;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public Duration getConnectionTimeout() {
    return connectionTimeout;
  }

  public void setConnectionTimeout(Duration connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
  }

  public Duration getRetryBackoff() {
    return retryBackoff;
  }

  public void setRetryBackoff(Duration retryBackoff) {
    this.retryBackoff = retryBackoff;
  }

  public int getMaximumPoolSize() {
    return maximumPoolSize;
  }

  public void setMaximumPoolSize(int maximumPoolSize) {
    this.maximumPoolSize = maximumPoolSize;
  }

  public List<DbLogTableMappingProperties> getTableMappings() {
    return tableMappings;
  }
}
