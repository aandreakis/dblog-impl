package io.github.aandreakis.dblog.integration.versionmatrix;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration-version-matrix")
class VersionMatrixConfigurationIT {
  @Test
  void defaultsToAcceptedMysqlAndPostgresVersionFamilies() {
    String previousMysql = System.getProperty("dblog.versionMatrix.mysqlImages");
    String previousPostgres = System.getProperty("dblog.versionMatrix.postgresImages");
    System.clearProperty("dblog.versionMatrix.mysqlImages");
    System.clearProperty("dblog.versionMatrix.postgresImages");
    try {
      assertThat(VersionMatrixImages.readMySqlImages())
          .containsExactlyElementsOf(VersionMatrixImages.DEFAULT_MYSQL_IMAGES);
      assertThat(VersionMatrixImages.readPostgresImages())
          .containsExactlyElementsOf(VersionMatrixImages.DEFAULT_POSTGRES_IMAGES);
    } finally {
      restore("dblog.versionMatrix.mysqlImages", previousMysql);
      restore("dblog.versionMatrix.postgresImages", previousPostgres);
    }
  }

  @Test
  void acceptsExplicitMysqlAndPostgresImageOverridesOnly() {
    System.setProperty("dblog.versionMatrix.mysqlImages", "mysql:8.0,mysql:8.4,mysql:9.6");
    System.setProperty("dblog.versionMatrix.postgresImages", "postgres:14,postgres:18");
    try {
      assertThat(VersionMatrixImages.readMySqlImages())
          .containsExactly("mysql:8.0", "mysql:8.4", "mysql:9.6");
      assertThat(VersionMatrixImages.readPostgresImages())
          .containsExactly("postgres:14", "postgres:18");
    } finally {
      System.clearProperty("dblog.versionMatrix.mysqlImages");
      System.clearProperty("dblog.versionMatrix.postgresImages");
    }
  }

  private static void restore(String propertyName, String value) {
    if (value == null) {
      System.clearProperty(propertyName);
    } else {
      System.setProperty(propertyName, value);
    }
  }
}
