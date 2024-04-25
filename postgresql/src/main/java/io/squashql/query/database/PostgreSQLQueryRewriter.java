package io.squashql.query.database;

import io.squashql.type.FunctionTypedField;

class PostgreSQLQueryRewriter implements QueryRewriter {

  @Override
  public String tableName(String table) {
    return SqlUtils.doubleQuoteEscape(table);
  }

  @Override
  public String cteName(String cteName) {
    return SqlUtils.doubleQuoteEscape(cteName);
  }

  @Override
  public String fieldName(String field) {
    return SqlUtils.doubleQuoteEscape(field);
  }

  @Override
  public String escapeAlias(String alias) {
    return SqlUtils.doubleQuoteEscape(alias);
  }

  @Override
  public boolean usePartialRollupSyntax() {
    return true;
  }

  @Override
  public boolean useAliasInHavingClause() {
    return false;
  }

  @Override
  public String functionExpression(FunctionTypedField ftf) {
    if ("current_date".contains(ftf.function())) {
      return "CURRENT_DATE";
    } else {
      return QueryRewriter.super.functionExpression(ftf);
    }
  }
}
