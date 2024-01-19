package io.squashql.query.database;

public class DefaultQueryRewriter implements QueryRewriter {

  private final DatabaseQuery query;

  public DefaultQueryRewriter(DatabaseQuery query) {
    this.query = query;
  }

  @Override
  public DatabaseQuery query() {
    return this.query;
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
  public String cteName(String cteName) {
    return SqlUtils.backtickEscape(cteName);
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
