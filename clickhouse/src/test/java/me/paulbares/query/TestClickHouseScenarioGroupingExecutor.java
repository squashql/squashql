package me.paulbares.query;

import com.clickhouse.jdbc.ClickHouseDataSource;
import me.paulbares.ClickHouseDatastore;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.ClickHouseTransactionManager;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;

import java.util.List;

import static me.paulbares.query.TestClickHouseQueryEngine.createClickHouseContainer;
import static me.paulbares.query.TestClickHouseQueryEngine.jdbcUrl;

public class TestClickHouseScenarioGroupingExecutor extends ATestScenarioGroupingExecutor {

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
    this.container.stop();
  }

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new ClickHouseQueryEngine((ClickHouseDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore(String storeName, List<Field> fields) {
    return new ClickHouseDatastore(jdbcUrl.apply(this.container), null);
  }

  @Override
  protected TransactionManager createTransactionManager() {
    return new ClickHouseTransactionManager((ClickHouseDataSource) this.datastore);
  }
}
