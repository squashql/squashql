package io.squashql.query;

import io.squashql.ClickHouseDatastore;
import io.squashql.query.database.ClickHouseQueryEngine;
import io.squashql.query.database.QueryEngine;
import io.squashql.store.Datastore;
import io.squashql.template.ClickHouseClassTemplateGenerator;
import io.squashql.transaction.ClickHouseTransactionManager;
import io.squashql.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;


/**
 * Do not edit this class, it has been generated automatically by {@link ClickHouseClassTemplateGenerator}.
 */
public class TestClickHouseDocDrillacross extends ATestDocDrillacross {

  public org.testcontainers.containers.GenericContainer container = TestUtils.createClickHouseContainer();

  @BeforeAll
  @Override
  void setup() {
    this.container.start();
    super.setup();
  }

  @AfterAll
  void tearDown() {
    // we do not stop the container to be able to reuse it between tests.
  }

  @Override
  protected void createTables() {
    ClickHouseTransactionManager tm = (ClickHouseTransactionManager) this.tm;
    this.fieldsByStore.forEach((store, fields) -> tm.dropAndCreateInMemoryTable(store, fields));
  }

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new ClickHouseQueryEngine((ClickHouseDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore() {
    return new ClickHouseDatastore(TestUtils.jdbcUrl.apply(this.container), null);
  }

  @Override
  protected TransactionManager createTransactionManager() {
    return new ClickHouseTransactionManager(((ClickHouseDatastore) this.datastore).dataSource);
  }
}
