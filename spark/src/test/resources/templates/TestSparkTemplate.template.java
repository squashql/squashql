package me.paulbares.query;

/**
 * Do not edit this class, it has been generated automatically by {@link me.paulbares.template.SparkClassTemplateGenerator}.
 */
public class TestSpark{{classSuffix}} extends {{parentTestClass}} {

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
  protected void createTables() {
    SparkTransactionManager tm = (SparkTransactionManager) this.tm;
    this.fieldsByStore.forEach((store, fields) -> tm.createTemporaryTable(store, fields));
  }
}
