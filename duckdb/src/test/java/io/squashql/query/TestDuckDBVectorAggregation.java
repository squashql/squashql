package io.squashql.query;

import io.squashql.DuckDBDatastore;
import io.squashql.query.database.DuckDBQueryEngine;
import io.squashql.query.database.QueryEngine;
import io.squashql.store.Datastore;
import io.squashql.transaction.DataLoader;
import io.squashql.transaction.DuckDBDataLoader;
import org.duckdb.DuckDBArray;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Do not edit this class, it has been generated automatically by {@link io.squashql.template.DuckDBClassTemplateGenerator}.
 */
public class TestDuckDBVectorAggregation extends ATestVectorAggregation {

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

  @Override
  protected List<Number> getVectorValue(Object actualVector) {
    if (actualVector instanceof DuckDBArray) {
      DuckDBArray v = (DuckDBArray) actualVector;
      try {
        Object[] a = (Object[]) v.getArray();
        List<Number> l = new ArrayList<>();
        for (int i = 0; i < a.length; i++) {
          l.add((Number) a[i]);
        }
        return l;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new IllegalArgumentException("expected instance of " + DuckDBArray.class + " but was " + actualVector.getClass());
    }
  }
}
