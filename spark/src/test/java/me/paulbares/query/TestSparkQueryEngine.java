package me.paulbares.query;

import me.paulbares.SparkDatastore;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;

import java.util.List;

public class TestSparkQueryEngine extends ATestQueryEngine {

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new SparkQueryEngine((SparkDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore(List<Field> fields) {
    return new SparkDatastore(fields);
  }
}
