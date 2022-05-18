package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.SparkStore;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;
import me.paulbares.transaction.TransactionManager;

import java.util.List;

public class TestSparkQueryEngineWithJoins extends ATestQueryEngineWithJoins {

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new SparkQueryEngine((SparkDatastore) datastore);
  }

  protected Store createStore(String storeName, List<Field> fields) {
    return new SparkStore(storeName, fields);
  }

  @Override
  protected Datastore createDatastore(List<Store> stores) {
    return new SparkDatastore();
  }

  @Override
  protected TransactionManager createTransactionManager() {
    throw new RuntimeException("nyi");
  }
}
