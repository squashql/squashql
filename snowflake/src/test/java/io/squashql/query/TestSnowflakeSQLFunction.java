package io.squashql.query;

import io.squashql.SnowflakeDatastore;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.SnowflakeQueryEngine;
import io.squashql.store.Datastore;
import io.squashql.transaction.DataLoader;
import io.squashql.transaction.SnowflakeDataLoader;
import org.junit.jupiter.api.AfterAll;

/**
 * Do not edit this class, it has been generated automatically by {@link io.squashql.template.SnowflakeClassTemplateGenerator}.
 */
public class TestSnowflakeSQLFunction extends ATestSQLFunction {

  @AfterAll
  void tearDown() {
    SnowflakeDataLoader tm = (SnowflakeDataLoader) this.tm;
    this.fieldsByStore.forEach((storeName, storeFields) -> tm.dropTable(storeName));
  }

  @Override
  protected void createTables() {
    SnowflakeDataLoader tm = (SnowflakeDataLoader) this.tm;
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
            SnowflakeTestUtil.database,
            SnowflakeTestUtil.schema,
            SnowflakeTestUtil.properties
    );
  }

  @Override
  protected DataLoader createDataLoader() {
    return new SnowflakeDataLoader((SnowflakeDatastore) this.datastore);
  }

  @Override
  protected Object translate(Object o) {
    return SnowflakeTestUtil.translate(o);
  }
}
