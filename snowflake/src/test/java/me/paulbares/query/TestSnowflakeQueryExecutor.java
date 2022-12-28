package me.paulbares.query;

import me.paulbares.SnowflakeDatastore;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.database.SnowflakeEngine;
import me.paulbares.store.Datastore;
import me.paulbares.transaction.SnowflakeTransactionManager;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;

public class TestSnowflakeQueryExecutor extends ATestQueryExecutor {

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
    return new SnowflakeEngine((SnowflakeDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore() {
    // TODO: set configuration
    return new SnowflakeDatastore(
            "jdbc:snowflake://<account_identifier>.snowflakecomputing.com",
            "username",
            "password",
            "warehouse",
            "database",
            "schema"
    );
  }

  @Override
  protected TransactionManager createTransactionManager() {
    return new SnowflakeTransactionManager((SnowflakeDatastore) this.datastore);
  }
}
