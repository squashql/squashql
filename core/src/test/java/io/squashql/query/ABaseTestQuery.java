package io.squashql.query;

import io.squashql.query.database.QueryEngine;
import io.squashql.store.Datastore;
import io.squashql.store.Field;
import io.squashql.transaction.DataLoader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ABaseTestQuery {

  protected Datastore datastore;
  protected QueryEngine queryEngine;
  protected DataLoader tm;
  protected QueryExecutor executor;
  protected Map<String, List<Field>> fieldsByStore;

  @BeforeAll
  void setup() {
    this.datastore = createDatastore();
    this.tm = createDataLoader();
    this.fieldsByStore = getFieldsByStore();

    createTables();
    // Create the engine after the tables because some components (such as AQueryEngine#datastore.storesByName()) need
    // to know the list of tables in advance.
    this.queryEngine = createQueryEngine(this.datastore);
    this.executor = new QueryExecutor(this.queryEngine);
    loadData();
    afterSetup();
  }

  protected void afterSetup() {
  }

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore();

  protected abstract DataLoader createDataLoader();

  protected abstract Map<String, List<Field>> getFieldsByStore();

  protected abstract void createTables();

  protected abstract void loadData();

  protected Object translate(Object o) {
    return o;
  }
}
