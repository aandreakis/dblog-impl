package io.github.aandreakis.dblog.testsupport;

import java.util.ArrayList;
import java.util.List;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/** Shared Testcontainers wiring for PostgreSQL tests, including logical CDC sources. */
public final class LivePostgresTestContainers {
  public static final String DEFAULT_IMAGE = "postgres:18";
  public static final String DEFAULT_DATABASE_NAME = "appdb";
  public static final String DEFAULT_USERNAME = "postgres";
  public static final String DEFAULT_PASSWORD = "postgres";
  public static final String DEFAULT_MAX_REPLICATION_SLOTS = "8";
  public static final String DEFAULT_MAX_WAL_SENDERS = "8";

  private LivePostgresTestContainers() {}

  public static PostgreSQLContainer newDefaultContainer() {
    return newContainer(DockerImageName.parse(DEFAULT_IMAGE));
  }

  public static PostgreSQLContainer newBaseContainer() {
    return configureBaseContainer(new PostgreSQLContainer(DockerImageName.parse(DEFAULT_IMAGE)));
  }

  public static PostgreSQLContainer newContainer(DockerImageName imageName) {
    return newContainer(imageName, DEFAULT_MAX_REPLICATION_SLOTS, DEFAULT_MAX_WAL_SENDERS);
  }

  public static PostgreSQLContainer newContainer(
      DockerImageName imageName,
      String maxReplicationSlots,
      String maxWalSenders,
      String... extraPostgresOptions) {
    return configureLiveCdcContainer(
        new PostgreSQLContainer(imageName),
        maxReplicationSlots,
        maxWalSenders,
        extraPostgresOptions);
  }

  public static PostgreSQLContainer configureBaseContainer(PostgreSQLContainer postgres) {
    return postgres
        .withDatabaseName(DEFAULT_DATABASE_NAME)
        .withUsername(DEFAULT_USERNAME)
        .withPassword(DEFAULT_PASSWORD);
  }

  public static PostgreSQLContainer configureLiveCdcContainer(PostgreSQLContainer postgres) {
    return configureLiveCdcContainer(
        postgres, DEFAULT_MAX_REPLICATION_SLOTS, DEFAULT_MAX_WAL_SENDERS);
  }

  public static PostgreSQLContainer configureLiveCdcContainer(
      PostgreSQLContainer postgres,
      String maxReplicationSlots,
      String maxWalSenders,
      String... extraPostgresOptions) {
    return configureBaseContainer(postgres)
        .withCommand(liveCdcCommand(maxReplicationSlots, maxWalSenders, extraPostgresOptions));
  }

  public static String[] liveCdcCommand(
      String maxReplicationSlots, String maxWalSenders, String... extraPostgresOptions) {
    List<String> command =
        new ArrayList<>(
            List.of(
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_replication_slots=" + maxReplicationSlots,
                "-c",
                "max_wal_senders=" + maxWalSenders));
    command.addAll(List.of(extraPostgresOptions));
    return command.toArray(String[]::new);
  }
}
