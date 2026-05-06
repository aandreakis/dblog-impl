package io.github.aandreakis.dblog.sink.api;

import io.github.aandreakis.dblog.core.schema.TableSchema;
import java.util.List;

/**
 * Optional sink-side preflight hook for validating compatibility with the captured-source
 * contract schemas. Implemented when the downstream has its own typed shape so a misalignment
 * is reported at startup rather than on the first appended batch.
 *
 * <p>Implementations must throw a runtime exception describing the mismatch; the runtime
 * propagates it as a fatal startup failure.
 */
public interface SinkSchemaValidator {
  void validateCapturedSchemas(List<TableSchema> capturedSchemas);
}
