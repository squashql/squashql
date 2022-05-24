package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.SparkTransactionManager;
import me.paulbares.transaction.TransactionManager;

import java.util.List;

public class TestSparkQueryEngine extends ATestQueryEngine {

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new SparkQueryEngine((SparkDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore() {
    return new SparkDatastore();
  }

  @Override
  protected TransactionManager createTransactionManager() {
    SparkDatastore ds = (SparkDatastore) this.datastore;
    return new SparkTransactionManager(ds.spark);
  }

  @Override
  protected void beforeLoading(List<Field> fields) {
    SparkTransactionManager tm = (SparkTransactionManager) this.tm;
    tm.createTemporaryTable(this.storeName, fields);
  }
}
