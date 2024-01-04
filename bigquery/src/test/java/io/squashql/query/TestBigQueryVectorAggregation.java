package io.squashql.query;

import com.google.cloud.bigquery.FieldValueList;
import io.squashql.BigQueryDatastore;
import io.squashql.BigQueryServiceAccountDatastore;
import io.squashql.query.database.BigQueryEngine;
import io.squashql.query.database.QueryEngine;
import io.squashql.store.Datastore;
import io.squashql.transaction.BigQueryDataLoader;
import io.squashql.transaction.DataLoader;
import org.junit.jupiter.api.AfterAll;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static io.squashql.query.BigQueryTestUtil.DATASET_NAME;
import static io.squashql.query.BigQueryTestUtil.PROJECT_ID;

/**
 * Do not edit this class, it has been generated automatically by {@link io.squashql.template.BigQueryClassTemplateGenerator}.
 */
public class TestBigQueryVectorAggregation extends ATestVectorAggregation {

  @AfterAll
  void tearDown() {
    this.fieldsByStore.forEach((store, fields) -> BigQueryTestUtil.deleteTable(((BigQueryDatastore) this.datastore).getBigquery(), store));
  }

  @Override
  protected void createTables() {
    BigQueryDataLoader tm = (BigQueryDataLoader) this.tm;
    BigQueryTestUtil.createDatasetIfDoesNotExist(tm.getBigQuery(), DATASET_NAME);
    this.fieldsByStore.forEach((store, fields) -> tm.dropAndCreateInMemoryTable(store, fields));
  }

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new BigQueryEngine((BigQueryDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore() {
    return new BigQueryServiceAccountDatastore(BigQueryTestUtil.createServiceAccountCredentials(), PROJECT_ID, DATASET_NAME);
  }

  @Override
  protected DataLoader createDataLoader() {
    return new BigQueryDataLoader(((BigQueryDatastore) this.datastore).getBigquery(), DATASET_NAME);
  }

  @Override
  protected Object translate(Object o) {
    return BigQueryTestUtil.translate(o);
  }

  @Override
  protected List<Number> getVectorValue(Object actualVector) {
    List<Number> r = new ArrayList<>();
    ((FieldValueList) actualVector).forEach(e -> r.add(new BigDecimal((String) e.getValue()).doubleValue()));
    return r;
  }
}
