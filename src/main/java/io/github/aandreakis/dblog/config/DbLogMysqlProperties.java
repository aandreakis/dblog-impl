package io.github.aandreakis.dblog.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

/** MySQL source configuration. */
public class DbLogMysqlProperties {
  private String jdbcUrl;
  private String databaseName;
  private String username;
  private String password = "";
  private String hostname;
  private Integer port;
  private Long serverId = 223344L;
  @NotNull private Duration connectTimeout = Duration.ofSeconds(5);
  @NotNull private Duration heartbeatInterval = Duration.ofSeconds(5);
  @NotNull private Duration keepAliveInterval = Duration.ofSeconds(300);
  @NotNull private Duration netWriteTimeout = Duration.ofMinutes(10);
  private boolean retryLogConnectionLoss = false;
  @NotNull private Duration reconnectBackoff = Duration.ofSeconds(3);

  @Min(1)
  private int sourceEventQueueCapacity = 50_000;

  public String getJdbcUrl() { return jdbcUrl; }
  public void setJdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }
  public String getDatabaseName() { return databaseName; }
  public void setDatabaseName(String databaseName) { this.databaseName = databaseName; }
  public String getUsername() { return username; }
  public void setUsername(String username) { this.username = username; }
  public String getPassword() { return password; }
  public void setPassword(String password) { this.password = password; }
  public String getHostname() { return hostname; }
  public void setHostname(String hostname) { this.hostname = hostname; }
  public Integer getPort() { return port; }
  public void setPort(Integer port) { this.port = port; }
  public Long getServerId() { return serverId; }
  public void setServerId(Long serverId) { this.serverId = serverId; }
  public Duration getConnectTimeout() { return connectTimeout; }
  public void setConnectTimeout(Duration connectTimeout) { this.connectTimeout = connectTimeout; }
  public Duration getHeartbeatInterval() { return heartbeatInterval; }
  public void setHeartbeatInterval(Duration heartbeatInterval) { this.heartbeatInterval = heartbeatInterval; }
  public Duration getKeepAliveInterval() { return keepAliveInterval; }
  public void setKeepAliveInterval(Duration keepAliveInterval) { this.keepAliveInterval = keepAliveInterval; }
  public Duration getNetWriteTimeout() { return netWriteTimeout; }
  public void setNetWriteTimeout(Duration netWriteTimeout) { this.netWriteTimeout = netWriteTimeout; }
  public boolean isRetryLogConnectionLoss() { return retryLogConnectionLoss; }
  public void setRetryLogConnectionLoss(boolean retryLogConnectionLoss) { this.retryLogConnectionLoss = retryLogConnectionLoss; }
  public Duration getReconnectBackoff() { return reconnectBackoff; }
  public void setReconnectBackoff(Duration reconnectBackoff) { this.reconnectBackoff = reconnectBackoff; }
  public int getSourceEventQueueCapacity() { return sourceEventQueueCapacity; }
  public void setSourceEventQueueCapacity(int sourceEventQueueCapacity) { this.sourceEventQueueCapacity = sourceEventQueueCapacity; }
}
