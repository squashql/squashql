package me.paulbares.query;

import me.paulbares.ClickHouseDatastore;
import me.paulbares.query.database.ClickHouseQueryEngine;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.ClickHouseTransactionManager;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;

import java.util.List;

import static me.paulbares.query.TestUtils.createClickHouseContainer;
import static me.paulbares.query.TestUtils.jdbcUrl;

public class TestClickHousePeriodComparison extends ATestPeriodComparison {

  @Container
  public GenericContainer container = createClickHouseContainer();

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
  protected void beforeLoading(List<Field> fields) {
    ClickHouseTransactionManager tm = (ClickHouseTransactionManager) this.tm;
    tm.dropAndCreateInMemoryTable(this.storeName, fields);
  }

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new ClickHouseQueryEngine((ClickHouseDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore() {
    return new ClickHouseDatastore(jdbcUrl.apply(this.container), null);
  }

  @Override
  protected TransactionManager createTransactionManager() {
    return new ClickHouseTransactionManager(((ClickHouseDatastore) this.datastore).dataSource);
  }
}
