package me.paulbares.query;

import me.paulbares.ClickHouseDatastore;
import me.paulbares.ClickHouseStore;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;

import java.util.List;

public class TestClickHouseScenarioGroupingExecutor extends ATestScenarioGroupingExecutor {

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new ClickHouseQueryEngine((ClickHouseDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore(List<Field> fields) {
    return new ClickHouseDatastore(new ClickHouseStore("storeName", fields));
  }
}
