package io.github.aandreakis.dblog.config.validation;

import io.github.aandreakis.dblog.config.DbLogProperties;
import java.util.List;
import java.util.Locale;

final class DbLogPropertiesValidationSupport {
  private DbLogPropertiesValidationSupport() {}

  static boolean hasText(String value) {
    return value != null && !value.isBlank();
  }

  static boolean hasNonBlankEntries(List<String> values) {
    if (values == null || values.isEmpty()) {
      return false;
    }
    return values.stream().anyMatch(DbLogPropertiesValidationSupport::hasText);
  }

  static boolean usesTwoPartNames(List<String> values) {
    if (values == null) {
      return false;
    }
    for (String value : values) {
      if (!hasText(value)) {
        continue;
      }
      int dot = value.indexOf('.');
      if (dot <= 0 || dot == value.length() - 1 || value.indexOf('.', dot + 1) >= 0) {
        return false;
      }
    }
    return true;
  }

  static String normalizedAdapter(String adapter) {
    if (!hasText(adapter)) {
      return "";
    }
    return switch (adapter.trim().toLowerCase(Locale.ROOT)) {
      case "postgresql" -> "postgres";
      default -> adapter.trim().toLowerCase(Locale.ROOT);
    };
  }

  static boolean isSupportedAdapter(String adapter) {
    String normalized = normalizedAdapter(adapter);
    return !normalized.isBlank()
        && (normalized.equals("mysql")
            || normalized.equals("postgres"));
  }

  static boolean hasExplicitOutputSink(DbLogProperties properties) {
    if (properties == null) {
      return false;
    }
    DbLogProperties.Sink sink = properties.getSink();
    return sink.getNdjson().isStdout()
        || sink.getNdjson().getPath() != null
        || sink.getTypedH2().getPath() != null
        || sink.getNoop().isEnabled()
        || properties.getTarget().isEnabled();
  }
}
