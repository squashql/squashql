package me.paulbares.query;

import me.paulbares.BigQueryDatastore;
import me.paulbares.BigQueryUtil;
import me.paulbares.query.database.BigQueryEngine;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.BigQueryTransactionManager;
import me.paulbares.transaction.TransactionManager;

import java.util.List;

import static me.paulbares.query.BigQueryTestUtil.PROJECT_ID;

public class TestBigQueryQueryExecutor extends ATestQueryExecutor {

  String datasetName = "testdataset";

  @Override
  protected void beforeLoading(List<Field> fields) {
    BigQueryTransactionManager tm = (BigQueryTransactionManager) this.tm;
    BigQueryTestUtil.createDatasetIfDoesNotExist(tm.getBigQuery(), this.datasetName);
    tm.dropAndCreateInMemoryTable(this.storeName, fields);
  }

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new BigQueryEngine((BigQueryDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore() {
    return new BigQueryDatastore(BigQueryUtil.createCredentials(BigQueryTestUtil.CREDENTIALS), PROJECT_ID, this.datasetName);
  }

  @Override
  protected TransactionManager createTransactionManager() {
    return new BigQueryTransactionManager(((BigQueryDatastore) this.datastore).getBigquery(), this.datasetName);
  }
}
