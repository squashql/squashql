package me.paulbares.query;

import me.paulbares.ClickHouseDatastore;
import me.paulbares.query.database.ClickHouseDeltaQueryEngine;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.store.Datastore;
import me.paulbares.transaction.ClickHouseDeltaTransactionManager;
import me.paulbares.transaction.TransactionManager;

import java.util.ArrayList;
import java.util.List;

import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;

public class TestClickHouseDeltaQueryExecutor extends TestClickHouseQueryExecutor {

  @Override
  protected void load() {
    this.tm.load(MAIN_SCENARIO_NAME, this.storeName, List.of(
            new Object[]{"bottle", "drink", 2d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    // Only the delta
    List<Object[]> es = new ArrayList<>();
    es.add(new Object[]{"bottle", "drink", 4d, 10});
    this.tm.load("s1", this.storeName, es);

    // Only the delta
    es.clear();
    es.add(new Object[]{"bottle", "drink", 1.5d, 10});
    this.tm.load("s2", this.storeName, es);
  }

  @Override
  protected TransactionManager createTransactionManager() {
    return new ClickHouseDeltaTransactionManager(((ClickHouseDatastore) this.datastore).dataSource);
  }

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new ClickHouseDeltaQueryEngine((ClickHouseDatastore) datastore);
  }
}