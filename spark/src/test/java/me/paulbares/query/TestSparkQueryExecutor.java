package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.database.SparkQueryEngine;
import me.paulbares.store.Datastore;
import me.paulbares.store.TypedField;
import me.paulbares.transaction.SparkTransactionManager;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TestSparkQueryExecutor extends ATestQueryExecutor {

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
    SparkDatastore ds = (SparkDatastore) this.datastore;
    return new SparkTransactionManager(ds.spark);
  }

  @Override
  protected void beforeLoad(List<TypedField> fields) {
    SparkTransactionManager tm = (SparkTransactionManager) this.tm;
    tm.createTemporaryTable(this.storeName, fields);
  }

  @Test
  @Disabled
  void testSubQueryWithCondition() {
    // FIXME issue with Spark:
    // org.apache.spark.sql.AnalysisException: Reference 'scenario' is ambiguous, could be: __temp_table__.scenario, myAwesomeStore.scenario.; line 1 pos 234
  }
}
