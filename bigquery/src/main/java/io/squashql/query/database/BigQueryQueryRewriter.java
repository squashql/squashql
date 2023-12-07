package io.squashql.query.database;

import io.squashql.query.date.DateFunctions;
import io.squashql.type.FunctionTypedField;

public class BigQueryQueryRewriter implements QueryRewriter {

  private final String projectId;
  private final String datasetName;

  BigQueryQueryRewriter(String projectId, String datasetName) {
    this.projectId = projectId;
    this.datasetName = datasetName;
  }

  @Override
  public String functionExpression(FunctionTypedField ftf) {
    if (DateFunctions.SUPPORTED_DATE_FUNCTIONS.contains(ftf.function())) {
      // https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#extract
      return String.format("EXTRACT(%s FROM %s)", ftf.function(), getFieldFullName(ftf.field()));
    } else {
      throw new IllegalArgumentException("Unsupported function " + ftf);
    }
  }

  @Override
  public String fieldName(String field) {
    return SqlUtils.backtickEscape(field);
  }

  @Override
  public String tableName(String table) {
    return SqlUtils.backtickEscape(this.projectId + "." + this.datasetName + "." + table);
  }

  /**
   * See <a href="https://cloud.google.com/bigquery/docs/schemas#column_names">https://cloud.google.com/bigquery/docs/schemas#column_names</a>.
   * A column name must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_), and it must start with a
   * letter or underscore. The maximum column name length is 300 characters.
   * FIXME must used a regex instead to replace incorrect characters.
   */
  @Override
  public String measureAlias(String alias) {
    return SqlUtils.backtickEscape(alias)
            .replace("(", "_")
            .replace(")", "_")
            .replace(" ", "_");
  }

  @Override
  public boolean usePartialRollupSyntax() {
    // Not supported https://issuetracker.google.com/issues/35905909
    return false;
  }

  @Override
  public String escapeSingleQuote(String s) {
    return SqlUtils.escapeSingleQuote(s, "\\'");
  }
}
