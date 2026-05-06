package io.github.aandreakis.dblog.integration.versionmatrix;

import java.util.Arrays;
import java.util.List;
import org.testcontainers.utility.DockerImageName;

final class VersionMatrixImages {
  static final List<String> DEFAULT_MYSQL_IMAGES =
      List.of("mysql:8.0", "mysql:8.4", "mysql:9.6");
  static final List<String> DEFAULT_POSTGRES_IMAGES =
      List.of("postgres:14", "postgres:15", "postgres:16", "postgres:17", "postgres:18");

  private VersionMatrixImages() {}

  static List<String> readMySqlImages() {
    return readImageList("dblog.versionMatrix.mysqlImages", DEFAULT_MYSQL_IMAGES);
  }

  static List<String> readPostgresImages() {
    return readImageList("dblog.versionMatrix.postgresImages", DEFAULT_POSTGRES_IMAGES);
  }

  static List<String> readImageList(String propertyName, List<String> defaults) {
    String raw = System.getProperty(propertyName);
    if (raw == null || raw.isBlank()) {
      return defaults;
    }
    return Arrays.stream(raw.split(","))
        .map(String::trim)
        .filter(value -> !value.isEmpty())
        .map(DockerImageName::parse)
        .map(DockerImageName::asCanonicalNameString)
        .toList();
  }

  static String imageLabel(String imageName) {
    return imageName.replace(':', '_').replace('/', '_');
  }
}
