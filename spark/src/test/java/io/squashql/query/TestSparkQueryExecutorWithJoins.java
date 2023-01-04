package io.squashql.query;

import io.squashql.SparkDatastore;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.SparkQueryEngine;
import io.squashql.store.Datastore;
import io.squashql.transaction.SparkTransactionManager;
import io.squashql.transaction.TransactionManager;

public class TestSparkQueryExecutorWithJoins extends ATestQueryExecutorWithJoins {

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
