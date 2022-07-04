package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.database.SparkQueryEngine;
import me.paulbares.store.Datastore;
import me.paulbares.transaction.SparkTransactionManager;
import me.paulbares.transaction.TransactionManager;

public class TestSparkQueryEngineWithJoins extends ATestQueryEngineWithJoins {

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
    return new SparkTransactionManager(((SparkDatastore) this.datastore).spark);
  }
}
