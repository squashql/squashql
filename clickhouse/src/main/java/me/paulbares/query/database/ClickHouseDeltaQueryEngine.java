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


  // FIRST METHOD WITH WHERE NOT IN but it can be used only if 1 primary key. In fact it works with ClickHouse.
  @Override
  protected Table retrieveAggregates(DatabaseQuery query) {
    List<String> scenarios = List.of("s1", "s2");
    List<String> keys = List.of("ean", "category");

    String virtualTable = virtualTableStatementWhereNotIn(query.table.name, scenarios, keys);
//    String virtualTable = virtualTableStatementWhereNotExists(query.table.name, scenarios, keys);

    String sql = SQLTranslator.translate(query, null, this.fieldSupplier, new QueryRewriter() {
      // FIXME quick way to test it
      @Override
      public String tableName(String table) {
        return "(" + virtualTable + ") as a";
      }
    });
    return getResults(sql, this.datastore.dataSource, query);
  }

  public static String virtualTableStatementWhereNotIn(String baseTableName, List<String> scenarios, List<String> columnKeys) {
    List<String> vtScenarios = new ArrayList<>(scenarios.size());
    for (String scenarioName : scenarios) {
      String scenarioStoreName = TransactionManager.scenarioStoreName(baseTableName, scenarioName);
      String keys = String.join(",", columnKeys);
      String sql = "SELECT *, '" + scenarioName + "' AS " + TransactionManager.SCENARIO_FIELD_NAME + "\n" +
              "FROM " + baseTableName + " WHERE (" + keys + ") NOT IN ( SELECT " + keys + " FROM " + scenarioStoreName + " )\n" +
              "UNION ALL\n" +
              "SELECT *, '" + scenarioName + "' FROM " + scenarioStoreName + "";
      vtScenarios.add(sql);
    }
    String sqlBase = "SELECT *, '" + MAIN_SCENARIO_NAME + "' AS " + TransactionManager.SCENARIO_FIELD_NAME + " FROM " + baseTableName;

    String virtualTable = sqlBase;
    for (String vtScenario : vtScenarios) {
      virtualTable += "\n" + "UNION ALL\n" + vtScenario;
    }
    return virtualTable;
  }

  public static String virtualTableStatementWhereNotExists(String baseTableName, List<String> scenarios, List<String> columnKeys) {
    List<String> vtScenarios = new ArrayList<>(scenarios.size());
    for (String scenarioName : scenarios) {
      String scenarioStoreName = TransactionManager.scenarioStoreName(baseTableName, scenarioName);
      StringBuilder condition = new StringBuilder();
      for (int i = 0; i < columnKeys.size(); i++) {
        String key = columnKeys.get(i);
//        condition.append(baseTableName).append('.').append(key);
        condition.append(key);
        condition.append(" = ");
        condition.append(scenarioStoreName).append('.').append(key);
        if (i < columnKeys.size() - 1) {
          condition.append(" AND ");
        }
      }
      String sql = "SELECT *, '" + scenarioName + "' AS " + TransactionManager.SCENARIO_FIELD_NAME + "\n" +
              "FROM " + baseTableName + " WHERE NOT EXISTS ( SELECT 1 FROM " + scenarioStoreName + " WHERE " + condition + " )\n" +
              "UNION ALL\n" +
              "SELECT *, '" + scenarioName + "' FROM " + scenarioStoreName + "";
      vtScenarios.add(sql);
    }
    String sqlBase = "SELECT *, '" + MAIN_SCENARIO_NAME + "' AS " + TransactionManager.SCENARIO_FIELD_NAME + " FROM " + baseTableName;

    String virtualTable = sqlBase;
    for (String vtScenario : vtScenarios) {
      virtualTable += "\n" + "UNION ALL\n" + vtScenario;
    }
    return virtualTable;
  }
}
