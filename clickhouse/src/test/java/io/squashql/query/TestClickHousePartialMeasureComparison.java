package io.squashql.query;

import io.squashql.ClickHouseDatastore;
import io.squashql.query.database.ClickHouseQueryEngine;
import io.squashql.query.database.QueryEngine;
import io.squashql.store.Datastore;
import io.squashql.template.ClickHouseClassTemplateGenerator;
import io.squashql.transaction.ClickHouseDataLoader;
import io.squashql.transaction.DataLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Do not edit this class, it has been generated automatically by {@link ClickHouseClassTemplateGenerator}.
 */
public class TestClickHousePartialMeasureComparison extends ATestPartialMeasureComparison {

  public org.testcontainers.containers.GenericContainer container = ClickHouseTestUtil.createClickHouseContainer();

  @BeforeAll
  @Override
  void setup() {
    this.container.start();
    super.setup();
  }

  @AfterAll
  void tearDown() {
    // we do not stop the container to be able to reuse it between tests.
    ((ClickHouseDataLoader) this.tm).dropTables(this.fieldsByStore.keySet());
  }

  @Override
  protected void createTables() {
    ClickHouseDataLoader tm = (ClickHouseDataLoader) this.tm;
    this.fieldsByStore.forEach((store, fields) -> tm.dropAndCreateInMemoryTable(store, fields));
  }

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new ClickHouseQueryEngine((ClickHouseDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore() {
    return new ClickHouseDatastore(ClickHouseTestUtil.jdbcUrl.apply(this.container));
  }

  @Override
  protected DataLoader createDataLoader() {
    return new ClickHouseDataLoader(((ClickHouseDatastore) this.datastore).dataSource);
  }
}
