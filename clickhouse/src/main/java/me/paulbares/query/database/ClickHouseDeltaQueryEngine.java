package me.paulbares.query.database;

import me.paulbares.ClickHouseDatastore;
import me.paulbares.query.Table;
import me.paulbares.transaction.TransactionManager;

import java.util.ArrayList;
import java.util.List;

import static me.paulbares.query.database.ClickHouseQueryEngine.getResults;
import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;

public class ClickHouseDeltaQueryEngine extends AQueryEngine<ClickHouseDatastore> {

  public ClickHouseDeltaQueryEngine(ClickHouseDatastore datastore) {
    super(datastore);
  }

  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    // FIXME
    List<String> scenarios = List.of("s1", "s2");
    List<String> keys = List.of("ean");
//    List<String> keys = List.of("ean", "category");
    String storeName = query.table.name;

    List<String> vtScenarios = new ArrayList<>(scenarios.size());
    for (String scenarioName : scenarios) {
      String scenarioStoreName = TransactionManager.scenarioStoreName(storeName, scenarioName);
      String sql = "SELECT *, '" + scenarioName + "' AS " + TransactionManager.SCENARIO_FIELD_NAME + "\n" +
              "FROM " + storeName + " WHERE " + String.join(",", keys) + " NOT IN ( SELECT " + String.join(",", keys) + " FROM " + scenarioStoreName + " )\n" +
              "UNION ALL\n" +
              "SELECT *, '" + scenarioName + "' FROM " + scenarioStoreName + "";
      vtScenarios.add(sql);
    }
    String sqlBase = "SELECT *, '" + MAIN_SCENARIO_NAME + "' AS " + TransactionManager.SCENARIO_FIELD_NAME + " FROM " + storeName;

    String[] virtualTable = new String[]{sqlBase};
    for (String vtScenario : vtScenarios) {
      virtualTable[0] += "\n" + "UNION ALL\n" + vtScenario;
    }

    String sql = SQLTranslator.translate(query, null, this.fieldSupplier, new QueryRewriter() {
      // FIXME
      @Override
      public String tableName(String table) {
        return "(" + virtualTable[0] + ") as a";
      }
    });
    return getResults(sql, this.datastore.dataSource, query);
  }
}
