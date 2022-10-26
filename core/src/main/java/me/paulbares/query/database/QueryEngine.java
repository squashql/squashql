package me.paulbares.query.database;

import me.paulbares.query.Table;
import me.paulbares.store.Datastore;
import me.paulbares.store.TypedField;

import java.util.function.Function;

public interface QueryEngine<T extends Datastore> {

  String GRAND_TOTAL = "Grand Total";
  String TOTAL = "Total";

  Table execute(DatabaseQuery query);

  T datastore();

  Function<String, TypedField> getFieldSupplier();
}
