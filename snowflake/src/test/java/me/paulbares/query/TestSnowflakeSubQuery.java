package me.paulbares.query;

import me.paulbares.SnowflakeDatastore;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.database.SnowflakeQueryEngine;
import me.paulbares.store.Datastore;
import me.paulbares.transaction.SnowflakeTransactionManager;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;

/**
 * Do not edit this class, it has been generated automatically by {@link me.paulbares.template.SnowflakeClassTemplateGenerator}.
 */
public class TestSnowflakeSubQuery extends ATestSubQuery {

  @AfterAll
  void tearDown() {
    SnowflakeTransactionManager tm = (SnowflakeTransactionManager) this.tm;
    this.fieldsByStore.forEach((storeName, storeFields) -> tm.dropTable(storeName));
  }

  @Override
  protected void createTables() {
    SnowflakeTransactionManager tm = (SnowflakeTransactionManager) this.tm;
    this.fieldsByStore.forEach(tm::createOrReplaceTable);
  }

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new SnowflakeQueryEngine((SnowflakeDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore() {
    return new SnowflakeDatastore(
            SnowflakeTestUtil.jdbcUrl,
            SnowflakeTestUtil.username,
            SnowflakeTestUtil.password,
            SnowflakeTestUtil.warehouse,
            SnowflakeTestUtil.database,
            SnowflakeTestUtil.schema
    );
  }

  @Override
  protected TransactionManager createTransactionManager() {
    return new SnowflakeTransactionManager((SnowflakeDatastore) this.datastore);
  }

  @Override
  protected Object translate(Object o) {
    return SnowflakeTestUtil.translate(o);
  }
}
