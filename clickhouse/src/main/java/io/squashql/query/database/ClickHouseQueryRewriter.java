package io.squashql.query.database;

import io.squashql.type.TypedField;

public class ClickHouseQueryRewriter implements QueryRewriter {

  @Override
  public String fieldName(String field) {
    return SqlUtils.backtickEscape(field);
  }

  @Override
  public String escapeAlias(String alias) {
    return SqlUtils.backtickEscape(alias);
  }

  @Override
  public boolean usePartialRollupSyntax() {
    // Not supported as of now: https://github.com/ClickHouse/ClickHouse/issues/322#issuecomment-615087004
    // Tested with version https://github.com/ClickHouse/ClickHouse/tree/v22.10.2.11-stable
    return false;
  }

  @Override
  public String arrayContains(TypedField field, Object value) {
    return "has(" + field.sqlExpression(this) + ", " + value + ")";
  }
}
