package io.squashql.query.database;

import io.squashql.type.TypedField;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.squashql.query.date.DateFunctions.DATE_PATTERNS;

public interface QueryRewriter {

  default String getFieldFullName(TypedField f) {
    return SqlUtils.getFieldFullName(f.store() == null ? null : tableName(f.store()), fieldName(f.name()));
  }

  default String fieldName(String field) {
    return field;
  }

  default String tableName(String table) {
    return table;
  }

  /**
   * Customizes how to refer to a Common Table Expression (CTE) in the SQL statement.
   *
   * @param cteName the name of the CTE
   * @return the customized argument
   */
  default String cteName(String cteName) {
    return cteName;
  }

  /**
   * Customizes what's written in the SELECT statement AND GROUP BY for the given selected column.
   * See {@link SQLTranslator}.
   *
   * @param f field to use in select
   * @return the customized argument
   */
  default String select(TypedField f) {
    return getFieldFullName(f);
  }

  default String selectDate(TypedField f) {
    for (Pattern p : DATE_PATTERNS) {
      Matcher matcher = p.matcher(f.name());
      if (matcher.find()) {
        return matcher.group(1) + "(" + matcher.group(2) + ")"; //todo-181 should we wrap the second group in SqlUtil#getFullName ?
      }
    }
    throw new UnsupportedOperationException("Unsupported function: " + f.name());
  }

  /**
   * Customizes what's written within the ROLLUP function as argument for the given column. See {@link SQLTranslator}.
   *
   * @param f field to use in rollup
   * @return the customized argument
   */
  default String rollup(TypedField f) {
    return getFieldFullName(f);
  }

  default String measureAlias(String alias) {
    return alias;
  }

  /**
   * Indicates if statement such as:
   * <pre>
   * {@code
   * SELECT
   *     warehouse, product, SUM(quantity)
   * FROM
   *     inventory
   * GROUP BY warehouse, ROLLUP (product);
   * }
   * </pre> is supported by the query engine and so the syntax above can be used.
   * <br>
   * See examples of partial rollup here
   * <a href="https://www.sqlservertutorial.net/sql-server-basics/sql-server-rollup/">
   * https://www.sqlservertutorial.net/sql-server-basics/sql-server-rollup/
   * </a>.
   */
  boolean usePartialRollupSyntax();

  /**
   * Indicates the grouping function can be used to identify extra rows added by rollup. <br>
   * See documentation
   * <a href="https://learn.microsoft.com/en-us/sql/t-sql/functions/grouping-transact-sql?view=sql-server-ver16">
   * https://learn.microsoft.com/en-us/sql/t-sql/functions/grouping-transact-sql?view=sql-server-ver16
   * </a>.
   */
  boolean useGroupingFunction();
}
