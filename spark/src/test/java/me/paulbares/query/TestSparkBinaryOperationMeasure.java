package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.database.SparkQueryEngine;
import me.paulbares.store.Datastore;
import me.paulbares.store.TypedField;
import me.paulbares.transaction.SparkTransactionManager;
import me.paulbares.transaction.TransactionManager;

import java.util.List;

public class TestSparkBinaryOperationMeasure extends ATestBinaryOperationMeasure {

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
  protected void beforeLoad(List<TypedField> fields) {
    SparkTransactionManager tm = (SparkTransactionManager) this.tm;
    tm.createTemporaryTable(this.storeName, fields);
  }
}
