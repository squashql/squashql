package me.paulbares.query;

import me.paulbares.query.database.QueryEngine;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ABaseTestQuery {

  protected Datastore datastore;
  protected QueryEngine queryEngine;
  protected TransactionManager tm;
  protected QueryExecutor executor;
  protected Map<String, List<Field>> fieldsByStore;

  @BeforeAll
  void setup() {
    this.datastore = createDatastore();
    this.queryEngine = createQueryEngine(this.datastore);
    this.executor = new QueryExecutor(this.queryEngine);
    this.tm = createTransactionManager();
    this.fieldsByStore = getFieldsByStore();

    createTables();
    loadData();
    afterSetup();
  }

  protected void afterSetup() {
  }

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore();

  protected abstract TransactionManager createTransactionManager();

  protected abstract Map<String, List<Field>> getFieldsByStore();

  protected abstract void createTables();

  protected abstract void loadData();

  protected Object translate(Object o) {
    return o;
  }
}
