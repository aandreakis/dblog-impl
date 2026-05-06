package io.github.aandreakis.dblog.config;

import jakarta.validation.constraints.NotBlank;

/** Source-to-target table mapping configuration. */
public class DbLogTableMappingProperties {
  @NotBlank
  private String sourceSchema;
  @NotBlank
  private String sourceTable;
  private String targetSchema;
  private String targetTable;

  public String getSourceSchema() {
    return sourceSchema;
  }

  public void setSourceSchema(String sourceSchema) {
    this.sourceSchema = sourceSchema;
  }

  public String getSourceTable() {
    return sourceTable;
  }

  public void setSourceTable(String sourceTable) {
    this.sourceTable = sourceTable;
  }

  public String getTargetSchema() {
    return targetSchema;
  }

  public void setTargetSchema(String targetSchema) {
    this.targetSchema = targetSchema;
  }

  public String getTargetTable() {
    return targetTable;
  }

  public void setTargetTable(String targetTable) {
    this.targetTable = targetTable;
  }
}
