package me.paulbares.query;

import me.paulbares.query.database.DatabaseQuery;
import me.paulbares.store.Datastore;

public interface QueryEngine<T extends Datastore> {

  String GRAND_TOTAL = "Grand Total";
  String TOTAL = "Total";

  Table execute(DatabaseQuery query);

  T datastore();
}
