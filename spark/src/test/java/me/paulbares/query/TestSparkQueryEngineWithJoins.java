package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.SparkStore;
import me.paulbares.store.Datastore;
import me.paulbares.store.Store;

import java.util.List;

public class TestSparkQueryEngineWithJoins extends ATestQueryEngineWithJoins {

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new SparkQueryEngine((SparkDatastore) datastore);
  }

  protected Store createStore(String storeName) {
    return new SparkStore(storeName);
  }

  @Override
  protected Datastore createDatastore(List<Store> stores) {
    return new SparkDatastore(stores.toArray(new SparkStore[0]));
  }
}
