package me.paulbares.query;

import me.paulbares.ClickHouseDatastore;
import me.paulbares.ClickHouseStore;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;

public class TestClickHouseQueryEngine extends ATestQueryEngine {

  @BeforeAll
  void setupContainer() {

  }

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new ClickHouseQueryEngine((ClickHouseDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore(String storeName, List<Field> fields) {
    return new ClickHouseDatastore(new ClickHouseStore(storeName, fields));
  }
}
