package io.squashql.query.database;

import io.squashql.query.Table;
import io.squashql.store.Datastore;
import io.squashql.store.Field;

import java.util.List;
import java.util.function.Function;

public interface QueryEngine<T extends Datastore> {

  String GRAND_TOTAL = "Grand Total";
  String TOTAL = "Total";

  Table execute(DatabaseQuery query);

  Table executeRawSql(String sql);

  T datastore();

  Function<String, Field> getFieldSupplier();

  /**
   * Returns the list of supported aggregation functions by the underlying database.
   */
  List<String> supportedAggregationFunctions();

  QueryRewriter queryRewriter();
}
