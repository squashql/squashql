package io.squashql.query;

import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import io.squashql.BigQueryDatastore;
import io.squashql.BigQueryUtil;
import io.squashql.query.database.BigQueryEngine;
import io.squashql.query.database.QueryEngine;
import io.squashql.store.Datastore;
import io.squashql.transaction.BigQueryTransactionManager;
import io.squashql.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;

import static io.squashql.query.BigQueryTestUtil.DATASET_NAME;
import static io.squashql.query.BigQueryTestUtil.PROJECT_ID;

/**
 * Do not edit this class, it has been generated automatically by {@link io.squashql.template.BigQueryClassTemplateGenerator}.
 */
public class TestBigQueryParentComparisonWithOtherColumn extends ATestParentComparisonWithOtherColumn {

  @AfterAll
  void tearDown() {
    this.fieldsByStore.forEach((store, fields) -> BigQueryTestUtil.deleteTable(((BigQueryDatastore) this.datastore).getBigquery(), store));
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
