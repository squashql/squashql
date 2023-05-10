package io.squashql.query;

import io.squashql.DuckDBDatastore;
import io.squashql.query.ATestQueryExecutor;
import io.squashql.query.database.DuckDBQueryEngine;
import io.squashql.query.database.QueryEngine;
import io.squashql.store.Datastore;
import io.squashql.transaction.DuckDBTransactionManager;
import io.squashql.transaction.TransactionManager;

/**
 * Do not edit this class, it has been generated automatically by {@link io.squashql.template.DuckDBClassTemplateGenerator}.
 */
public class TestDuckDBBucketComparison extends ATestBucketComparison {

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new DuckDBQueryEngine((DuckDBDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore() {
    return new DuckDBDatastore();
  }

  @Override
  protected TransactionManager createTransactionManager() {
    return new DuckDBTransactionManager((DuckDBDatastore) this.datastore);
  }

  @Override
  protected void createTables() {
    DuckDBTransactionManager tm = (DuckDBTransactionManager) this.tm;
    this.fieldsByStore.forEach(tm::createOrReplaceTable);
  }
}
