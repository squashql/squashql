package io.squashql.query.database;

import io.squashql.type.TypedField;

public class SparkQueryRewriter extends DefaultQueryRewriter {

  public SparkQueryRewriter(DatabaseQuery query) {
    super(query);
  }

  @Override
  public String escapeSingleQuote(String s) {
    return SqlUtils.escapeSingleQuote(s, "\\'");
  }

  @Override
  public String grouping(TypedField f) {
    // Spark does not support using the alias. See https://github.com/squashql/squashql/issues/186
    return _select(f, false);
  }

  @Override
  public String rollup(TypedField f) {
    // Spark does not support using the alias. See See https://github.com/squashql/squashql/issues/186
    return _select(f, false);
  }

  @Override
  public String groupBy(TypedField f) {
    // Spark supports aliases in group by but not in rollup. That could cause inconsistency when generating the SQL.
    // For that reason, we do not use aliases in group by. See https://github.com/squashql/squashql/issues/186
    return _select(f, false);
  }
}
