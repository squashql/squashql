package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.database.SparkQueryEngine;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.SparkTransactionManager;
import me.paulbares.transaction.TransactionManager;

import java.util.List;
import java.util.Map;

public class TestSparkSubQuery extends ATestSubQuery {

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
  protected void beforeLoad(Map<String, List<Field>> fieldsByStore) {
    SparkTransactionManager tm = (SparkTransactionManager) this.tm;
    fieldsByStore.forEach((store, fields) -> tm.createTemporaryTable(store, fields));
  }
}