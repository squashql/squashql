package me.paulbares.query;

import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import me.paulbares.BigQueryDatastore;
import me.paulbares.BigQueryUtil;
import me.paulbares.query.database.BigQueryEngine;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.store.Datastore;
import me.paulbares.transaction.BigQueryTransactionManager;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;

import static me.paulbares.query.BigQueryTestUtil.DATASET_NAME;
import static me.paulbares.query.BigQueryTestUtil.PROJECT_ID;

/**
 * Do not edit this class, it has been generated automatically by {@link me.paulbares.template.BigQueryClassTemplateGenerator}.
 */
public class TestBigQueryBinaryOperationMeasure extends ATestBinaryOperationMeasure {

  @AfterAll
  void tearDown() {
    this.fieldsByStore.forEach((store, fields) -> {
      TableId tableId = TableId.of(DATASET_NAME, store);
      BigQueryDatastore ds = (BigQueryDatastore) datastore;
      Table table = ds.getBigquery().getTable(tableId);
      boolean deleted = table.delete();
      if (deleted) {
        // the table was deleted
        System.out.println("Table " + tableId + " successfully deleted");
      } else {
        // the table was not found
        System.out.println("Table " + tableId + " could not be deleted");
      }
    });
  }

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

  @Override
  protected Object translate(Object o) {
    return BigQueryTestUtil.translate(o);
  }
}
