package me.paulbares.query;

import com.google.common.base.Suppliers;
import me.paulbares.ClickHouseDatastore;
import me.paulbares.query.database.ClickHouseQueryEngine;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.store.Field;
import me.paulbares.transaction.ClickHouseTransactionManager;
import me.paulbares.transaction.TransactionManager;
import org.testcontainers.containers.GenericContainer;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ClickHouseTestConfigurer implements TestConfigurer {

  public GenericContainer container = TestUtils.createClickHouseContainer();

  protected Supplier<ClickHouseDatastore> datastore = Suppliers.memoize(() -> new ClickHouseDatastore(TestUtils.jdbcUrl.apply(this.container), null));

  @Override
  public void beforeSetup() {
    this.container.start();
  }

  @Override
  public QueryEngine createQueryEngine() {
    return new ClickHouseQueryEngine(this.datastore.get());
  }

  @Override
  public TransactionManager createTransactionManager() {
    return new ClickHouseTransactionManager(this.datastore.get().dataSource);
  }

  @Override
  public void createTables(TransactionManager transactionManager, Map<String, List<Field>> fieldsByStore) {
    ClickHouseTransactionManager tm = (ClickHouseTransactionManager) transactionManager;
    fieldsByStore.forEach((store, fields) -> tm.dropAndCreateInMemoryTable(store, fields));
  }
}
