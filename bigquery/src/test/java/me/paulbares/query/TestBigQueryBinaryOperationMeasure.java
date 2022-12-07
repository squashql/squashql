package me.paulbares.query;

import me.paulbares.BigQueryDatastore;
import me.paulbares.BigQueryUtil;
import me.paulbares.query.database.BigQueryEngine;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.store.Datastore;
import me.paulbares.transaction.BigQueryTransactionManager;
import me.paulbares.transaction.TransactionManager;

import static me.paulbares.query.BigQueryTestUtil.DATASET_NAME;
import static me.paulbares.query.BigQueryTestUtil.PROJECT_ID;

/**
 * Do not edit this class, it has been generated automatically by {@link me.paulbares.template.BigQueryClassTemplateGenerator}.
 */
public class TestBigQueryBinaryOperationMeasure extends ATestBinaryOperationMeasure {

  @Override
  protected void createTables() {
    BigQueryTransactionManager tm = (BigQueryTransactionManager) this.tm;
    BigQueryTestUtil.createDatasetIfDoesNotExist(tm.getBigQuery(), DATASET_NAME);
    this.fieldsByStore.forEach((store, fields) -> tm.dropAndCreateInMemoryTable(store, fields));
  }

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new BigQueryEngine((BigQueryDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore() {
    return new BigQueryDatastore(BigQueryUtil.createCredentials(BigQueryTestUtil.CREDENTIALS), PROJECT_ID, DATASET_NAME);
  }

  @Override
  protected TransactionManager createTransactionManager() {
    return new BigQueryTransactionManager(((BigQueryDatastore) this.datastore).getBigquery(), DATASET_NAME);
  }
}
