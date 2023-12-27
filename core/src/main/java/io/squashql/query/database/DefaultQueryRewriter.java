package io.squashql.query.database;

public class DefaultQueryRewriter implements QueryRewriter {

  public static final DefaultQueryRewriter INSTANCE = new DefaultQueryRewriter();

  public DefaultQueryRewriter() {
  }

  @Override
  public String fieldName(String field) {
    return SqlUtils.backtickEscape(field);
  }

  @Override
  public String tableName(String table) {
    return SqlUtils.backtickEscape(table);
  }

  @Override
  public String escapeAlias(String alias) {
    return SqlUtils.backtickEscape(alias);
  }

  @Override
  public boolean usePartialRollupSyntax() {
    return true;
  }
}
