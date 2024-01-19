package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;

public interface NamedTable extends CompiledTable {

  /**
   * The name of the table.
   */
  String name();

  /**
   * The sql expression to use for the name of the table.
   */
  String sqlExpressionTableName(QueryRewriter queryRewriter);
}
