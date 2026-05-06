package io.github.aandreakis.dblog.config;

import jakarta.validation.constraints.NotNull;
import java.time.Duration;

/** PostgreSQL source configuration. */
public class DbLogPostgresProperties {
  private String jdbcUrl;
  private String replicationJdbcUrl;
  private String databaseName;
  private String username;
  private String password = "";
  private String publicationName;
  private PostgresResourceOwnership publicationOwnership = PostgresResourceOwnership.DBLOG_MANAGED;
  private String slotName;
  private PostgresResourceOwnership slotOwnership = PostgresResourceOwnership.DBLOG_MANAGED;
  @NotNull private Duration statusInterval = Duration.ofSeconds(5);
  private boolean retryLogConnectionLoss = false;
  @NotNull private Duration reconnectBackoff = Duration.ofSeconds(3);

  public String getJdbcUrl() { return jdbcUrl; }
  public void setJdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }
  public String getReplicationJdbcUrl() { return replicationJdbcUrl; }
  public void setReplicationJdbcUrl(String replicationJdbcUrl) { this.replicationJdbcUrl = replicationJdbcUrl; }
  public String getDatabaseName() { return databaseName; }
  public void setDatabaseName(String databaseName) { this.databaseName = databaseName; }
  public String getUsername() { return username; }
  public void setUsername(String username) { this.username = username; }
  public String getPassword() { return password; }
  public void setPassword(String password) { this.password = password; }
  public String getPublicationName() { return publicationName; }
  public void setPublicationName(String publicationName) { this.publicationName = publicationName; }
  public PostgresResourceOwnership getPublicationOwnership() { return publicationOwnership; }
  public void setPublicationOwnership(PostgresResourceOwnership publicationOwnership) { this.publicationOwnership = publicationOwnership; }
  public String getSlotName() { return slotName; }
  public void setSlotName(String slotName) { this.slotName = slotName; }
  public PostgresResourceOwnership getSlotOwnership() { return slotOwnership; }
  public void setSlotOwnership(PostgresResourceOwnership slotOwnership) { this.slotOwnership = slotOwnership; }
  public Duration getStatusInterval() { return statusInterval; }
  public void setStatusInterval(Duration statusInterval) { this.statusInterval = statusInterval; }
  public boolean isRetryLogConnectionLoss() { return retryLogConnectionLoss; }
  public void setRetryLogConnectionLoss(boolean retryLogConnectionLoss) { this.retryLogConnectionLoss = retryLogConnectionLoss; }
  public Duration getReconnectBackoff() { return reconnectBackoff; }
  public void setReconnectBackoff(Duration reconnectBackoff) { this.reconnectBackoff = reconnectBackoff; }
}
