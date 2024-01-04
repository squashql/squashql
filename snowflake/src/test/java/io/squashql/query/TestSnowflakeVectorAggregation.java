package io.squashql.query;

import io.squashql.SnowflakeDatastore;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.database.SnowflakeQueryEngine;
import io.squashql.store.Datastore;
import io.squashql.transaction.DataLoader;
import io.squashql.transaction.SnowflakeDataLoader;
import org.junit.jupiter.api.AfterAll;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Do not edit this class, it has been generated automatically by {@link io.squashql.template.SnowflakeClassTemplateGenerator}.
 */
public class TestSnowflakeVectorAggregation extends ATestVectorAggregation {

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

  @Override
  protected List<Number> getVectorValue(Object actualVector) {
    // It is a string that needs to be parsed, see https://github.com/snowflakedb/snowflake-jdbc/issues/462
    String[] split = ((String) actualVector).replace("\n", "")
            .replace("[", "")
            .replace("]", "")
            .split(",");
    List<Number> r = new ArrayList<>();
    for (String s : split) {
      String trim = s.trim();
      BigDecimal bigDecimal = new BigDecimal(trim);
      double v = bigDecimal.doubleValue();
      r.add(v);
    }
    return r;
  }
}
