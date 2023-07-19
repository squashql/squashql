package io.squashql.query.database;

import io.squashql.query.QueryExecutor;
import io.squashql.table.Table;
import io.squashql.store.Datastore;
import io.squashql.store.TypedField;

import java.util.List;
import java.util.function.Function;

public interface QueryEngine<T extends Datastore> {

  String GRAND_TOTAL = "Grand Total";
  String TOTAL = "Total";

  Table execute(DatabaseQuery query, QueryExecutor.PivotTableContext context);

  Table executeRawSql(String sql);

  T datastore();

  Function<String, TypedField> getFieldSupplier();

  /**
   * Returns the list of supported aggregation functions by the underlying database.
   */
  List<String> supportedAggregationFunctions();

  QueryRewriter queryRewriter();
}
