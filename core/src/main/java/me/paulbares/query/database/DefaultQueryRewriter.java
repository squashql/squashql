package me.paulbares.query.database;

public class DefaultQueryRewriter implements QueryRewriter {

  public static final DefaultQueryRewriter INSTANCE = new DefaultQueryRewriter();

  private DefaultQueryRewriter() {
  }

  @Override
  public String select(String select) {
    return SqlUtils.escape(select);
  }

  @Override
  public String rollup(String rollup) {
    return SqlUtils.escape(rollup);
  }

  @Override
  public String measureAlias(String alias) {
    return SqlUtils.escape(alias);
  }

  @Override
  public boolean doesSupportPartialRollup() {
    return true;
  }

  @Override
  public boolean doesSupportGroupingFunction() {
    return true;
  }
}
