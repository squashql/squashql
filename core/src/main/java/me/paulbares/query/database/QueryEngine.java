package me.paulbares.query.database;

import me.paulbares.query.Table;
import me.paulbares.store.Datastore;

public interface QueryEngine<T extends Datastore> {

  String GRAND_TOTAL = "Grand Total";
  String TOTAL = "Total";

  Table execute(DatabaseQuery query);

  T datastore();
}
