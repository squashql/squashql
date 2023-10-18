package io.squashql.query.database;

import io.squashql.store.Datastore;
import io.squashql.table.Table;

import java.util.List;

public interface QueryEngine<T extends Datastore> {

  String GRAND_TOTAL = "Grand Total";
  String TOTAL = "Total";

  Table execute(DatabaseQuery query);

  Table executeRawSql(String sql);

  T datastore();

  /**
   * Returns the list of supported aggregation functions by the underlying database.
   */
  List<String> supportedAggregationFunctions();

  QueryRewriter queryRewriter();
}
