package me.paulbares.query;

import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;

public interface QueryEngine<T extends Datastore> {

  String GRAND_TOTAL = "Grand Total";
  String TOTAL = "Total";

  Table execute(QueryDto query);

  T datastore();
}
