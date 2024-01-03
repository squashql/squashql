package io.squashql.query;

import io.squashql.SparkDatastore;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.SparkQueryEngine;
import io.squashql.store.Datastore;
import io.squashql.transaction.DataLoader;
import io.squashql.transaction.SparkDataLoader;
import org.junit.jupiter.api.AfterAll;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.util.List;

/**
 * Do not edit this class, it has been generated automatically by {@link io.squashql.template.SparkClassTemplateGenerator}.
 */
public class TestSparkVectorAggregation extends ATestVectorAggregation {

  @AfterAll
  void tearDown() {
    this.fieldsByStore.keySet().forEach(storeName -> ((SparkDatastore) this.datastore).spark.catalog().dropTempView(storeName));
  }

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
    SparkDatastore ds = (SparkDatastore) this.datastore;
    return new SparkDataLoader(ds.spark);
  }

  @Override
  protected void createTables() {
    SparkDataLoader tm = (SparkDataLoader) this.tm;
    this.fieldsByStore.forEach((store, fields) -> tm.createTemporaryTable(store, fields));
  }

  @Override
  protected List<Number> getVectorValue(Object actualVector) {
    return JavaConverters.seqAsJavaList(((WrappedArray.ofRef) actualVector).seq());
  }
}
