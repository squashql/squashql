package io.squashql.query.database;

import io.squashql.type.FunctionTypedField;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;

public interface QueryRewriter {

  default String getFieldFullName(TableTypedField f) {
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

  default String functionExpression(FunctionTypedField ftf) {
    return ftf.function() + "(" + getFieldFullName(ftf.field()) + ")";
  }

  /**
   * Customizes what's written in the SELECT statement AND GROUP BY for the given selected column.
   * See {@link SQLTranslator}.
   *
   * @param f field to use in select
   * @return the customized argument
   */
  default String select(TypedField f) {
    if (f instanceof TableTypedField ttf) {
      return getFieldFullName(ttf);
    } else if (f instanceof FunctionTypedField ftf) {
      return functionExpression(ftf);
    } else {
      throw new IllegalArgumentException(f.getClass().getName());
    }
  }

  /**
   * Customizes what's written within the ROLLUP function as argument for the given column. See {@link SQLTranslator}.
   *
   * @param f field to use in rollup
   * @return the customized argument
   */
  default String rollup(TypedField f) {
    return select(f);
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

  default String escapeSingleQuote(String s) {
    return SqlUtils.escapeSingleQuote(s, "''");
  }
}
