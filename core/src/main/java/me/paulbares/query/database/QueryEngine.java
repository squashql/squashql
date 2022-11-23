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
}
