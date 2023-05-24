package io.squashql.query;

import io.squashql.SparkDatastore;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.SparkQueryEngine;
import io.squashql.store.Datastore;
import io.squashql.transaction.SparkDataLoader;
import io.squashql.transaction.DataLoader;

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
  protected DataLoader createDataLoader() {
    return new SparkDataLoader(((SparkDatastore) this.datastore).spark);
  }
}
