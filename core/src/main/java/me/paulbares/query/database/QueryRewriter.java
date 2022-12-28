package me.paulbares.query.database;

public interface QueryRewriter {

  default String tableName(String table) {
    return table;
  }

  /**
   * Customizes what's written in the SELECT statement AND GROUP BY for the given selected column.
   * See {@link SQLTranslator}.
   * @param select name of the column
   * @return the customized argument
   */
  default String select(String select) {
    return select;
  }

  /**
   * Customizes what's written within the ROLLUP function as argument for the given column. See {@link SQLTranslator}.
   * @param rollup name of the column in the rollup
   * @return the customized argument
   */
  default String rollup(String rollup) {
    return rollup;
  }

  default String measureAlias(String alias) {
    return alias;
  }

  boolean doesSupportPartialRollup();

  /**
   * See documentation
   * <a href="https://learn.microsoft.com/en-us/sql/t-sql/functions/grouping-transact-sql?view=sql-server-ver16>https://learn.microsoft.com/en-us/sql/t-sql/functions/grouping-transact-sql?view=sql-server-ver16</a>.
   */
  boolean doesSupportGroupingFunction();
}
