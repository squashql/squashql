package io.squashql.query;

import io.squashql.DuckDBDatastore;
import io.squashql.query.database.DuckDBQueryEngine;
import io.squashql.query.database.QueryEngine;
import io.squashql.store.Datastore;
import io.squashql.transaction.DataLoader;
import io.squashql.transaction.DuckDBDataLoader;

/**
 * Do not edit this class, it has been generated automatically by {@link io.squashql.template.DuckDBClassTemplateGenerator}.
 */
public class TestDuckDB{{classSuffix}} extends {{parentTestClass}} {

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new DuckDBQueryEngine((DuckDBDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore() {
    return new DuckDBDatastore();
  }

  @Override
  protected DataLoader createDataLoader() {
    return new DuckDBDataLoader((DuckDBDatastore) this.datastore);
  }

  @Override
  protected void createTables() {
    DuckDBDataLoader tm = (DuckDBDataLoader) this.tm;
    this.fieldsByStore.forEach(tm::createOrReplaceTable);
  }
}
