package me.paulbares.query;

import com.clickhouse.client.*;
import com.clickhouse.jdbc.ClickHouseDataSource;
import me.paulbares.ClickHouseDatastore;
import me.paulbares.ClickHouseUtil;
import me.paulbares.store.Field;
import me.paulbares.transaction.ClickHouseDeltaTransactionManager;
import me.paulbares.transaction.TransactionManager;
import org.eclipse.collections.api.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static me.paulbares.query.database.AQueryEngine.transform;
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

  @Test
  void name() {
    ClickHouseDataSource dataSource = ((ClickHouseDatastore) this.datastore).dataSource;
    ClickHouseNode server = ClickHouseNode.builder()
            .host(dataSource.getHost())
            .port(dataSource.getPort())
            .build();

    String scenarioName = "s1";
    String scenarioStoreName = ClickHouseDeltaTransactionManager.scenarioStoreName(this.storeName, scenarioName);
    String sqlS1 = "SELECT *, '" + scenarioName + "' AS " + TransactionManager.SCENARIO_FIELD_NAME + " FROM " + this.storeName + " WHERE ean NOT IN ( SELECT ean FROM " + scenarioStoreName + " )\n" +
            "UNION ALL\n" +
            "SELECT *, '" + scenarioName + "' FROM " + scenarioStoreName + "";
    String sqlBase = "SELECT *, '" + MAIN_SCENARIO_NAME + "' AS " + TransactionManager.SCENARIO_FIELD_NAME + " FROM " + this.storeName;
    String sql = sqlBase + "\n" + "UNION ALL\n" + sqlS1;

    try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
         ClickHouseResponse response = client.connect(server)
                 .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                 .query("SELECT scenario FROM (" + sql + ") as a")
                 .execute()
                 .get()) {
//      new ClickHouseResultSet(response.dat)
//      .forEach(r -> System.out.println(r.iterator().forEachRemaining(o -> System.out.print(o + ","))));
      Pair<List<Field>, List<List<Object>>> result = transform(response.getColumns(),
              c -> new Field(c.getColumnName(), ClickHouseUtil.clickHouseTypeToClass(c.getDataType())),
              response.records().iterator(),
              (i, r) -> r.getValue(i).asObject());
      System.out.println(response.getSummary().getStatistics());
      new ColumnarTable(
              result.getOne(),
              List.of(),
              new int[0],
              new int[0],
              result.getTwo()).show();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
