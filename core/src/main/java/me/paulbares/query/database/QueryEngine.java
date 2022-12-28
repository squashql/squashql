package me.paulbares.query.database;

import me.paulbares.query.Table;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;

import java.util.List;
import java.util.function.Function;

public interface QueryEngine<T extends Datastore> {

  String GRAND_TOTAL = "Grand Total";
  String TOTAL = "Total";

  Table execute(DatabaseQuery query);

  T datastore();

  Function<String, Field> getFieldSupplier();

  /**
   * Returns the list of supported aggregation functions by the underlying database.
   */
  List<String> supportedAggregationFunctions();

  /**
   * See examples of partial rollup here
   * <a href="https://www.sqlservertutorial.net/sql-server-basics/sql-server-rollup/">
   * https://www.sqlservertutorial.net/sql-server-basics/sql-server-rollup/
   * </a>
   *
   * @return true if the query engine support partial rollup
   */
//  boolean doesSupportPartialRollup();
//
//  /**
//   * See documentation
//   * <a href="https://learn.microsoft.com/en-us/sql/t-sql/functions/grouping-transact-sql?view=sql-server-ver16>https://learn.microsoft.com/en-us/sql/t-sql/functions/grouping-transact-sql?view=sql-server-ver16</a>.
//   */
//  boolean doesSupportGroupingFunction();
}
