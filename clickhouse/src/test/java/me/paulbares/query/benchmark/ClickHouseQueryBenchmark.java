package me.paulbares.query.benchmark;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.clickhouse.client.ClickHouseProtocol;
import me.paulbares.ClickHouseDatastore;
import me.paulbares.ClickHouseStore;
import me.paulbares.transaction.ClickHouseTransactionManager;
import me.paulbares.SparkStore;
import me.paulbares.benchmark.BenchmarkRunner;
import me.paulbares.query.ClickHouseQueryEngine;
import me.paulbares.query.DatasetTable;
import me.paulbares.query.Table;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

public class ClickHouseQueryBenchmark {

  static {
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.INFO);
    Logger org = (Logger) LoggerFactory.getLogger("org");
    org.setLevel(Level.WARN);
  }

  private static final String delimiter = ",";
  private static final boolean header = true;
  private static String ordersStore = "ordersStore";
  private static int N = 500;

  public static void main(String[] args) throws Exception {
    Pair<ClickHouseDatastore, DatastoreInfo> datastore = createAndLoadStore();
    ClickHouseQueryEngine queryEngine = new ClickHouseQueryEngine(datastore.getOne());
    List<Table> results = new ArrayList<>();
    BenchmarkRunner.INSTANCE.run(() -> {
      QueryDto query = new QueryDto().table(ordersStore)
              .wildcardCoordinate(SCENARIO_FIELD_NAME)
              .coordinates("CategoryName", "Condiments", "Beverages")
              .aggregatedMeasure("Price", "sum")
              .aggregatedMeasure("Quantity", "sum");
      Table result = queryEngine.execute(query);
      results.add(result);
    });
    String message = String.format("Query run %s times on a datastore with %s lines per scenario (x %s)",
            results.size(),
            datastore.getTwo().nbLines,
            datastore.getTwo().scenarios.size());
    System.out.println(message);
    System.out.println("Last result:");
    results.get(results.size() - 1).show();
  }

  static Pair<ClickHouseDatastore, DatastoreInfo> createAndLoadStore() {
    List<Field> fields = List.of(
            new Field("OrderId", long.class),
            new Field("CustomerID", long.class),
            new Field("EmployeeID", long.class),
            new Field("OrderDate", String.class),
            new Field("OrderDetailID", long.class),
            new Field("Quantity", int.class),
            new Field("ProductName", String.class),
            new Field("Unit", String.class),
            new Field("Price", double.class),
            new Field("CategoryName", String.class),
            new Field("SupplierName", String.class),
            new Field("City", String.class),
            new Field("Country", String.class),
            new Field("ShipperName", String.class));

    ClickHouseStore clickHouseStore = new ClickHouseStore(ordersStore, fields); // FIXME
    String jdbc = String.format("jdbc:clickhouse://%s:%d", "localhost", ClickHouseProtocol.HTTP.getDefaultPort());
    ClickHouseDatastore datastore = new ClickHouseDatastore(jdbc, null);

    ClickHouseTransactionManager tm = new ClickHouseTransactionManager(datastore.getDataSource());

    Function<String, Integer> loader = Dataloader.createScenarioLoader(tm, fields);
    List<String> scenarios = List.of(Datastore.MAIN_SCENARIO_NAME, "s50", "s25", "s10", "s05");
    int count = -1;
    for (String scenario : scenarios) {
      int c = loader.apply(scenario);
      if (count == -1) {
        count = c;
      }
    }

    return Tuples.pair(datastore, new DatastoreInfo(count, scenarios));
  }

  record DatastoreInfo(int nbLines, List<String> scenarios) {
  }

  public static class Dataloader {

    public static Function<String, Integer> createScenarioLoader(TransactionManager tm, List<Field> fields) {
      String path = "spark/src/test/resources/benchmark/data_%s_scenario.csv";
      Function<String, String> pathFunction = scenario -> String.format(path, scenario);

      SparkSession spark = SparkSession
              .builder()
              .appName("Java Spark SQL Example")
              .config("spark.master", "local")
              .getOrCreate();

      return scenario -> {
        Dataset<Row> ds = spark.read()
                .option("delimiter", delimiter)
                .option("header", header)
                .schema(SparkStore.createSchema(fields.toArray(new Field[0])))
                // use the schema to have tuples correctly formed otherwise
                // all elements are strings
                .csv(pathFunction.apply(scenario));

        int keyIndex = 4; // index of OrderDetailID

        // Transform to tuples
        int size = (int) ds.count();
        List<Object[]> tuples = new ArrayList<>(size * N);
        List<Object[]> basicTuples = new ArrayList<>(N);
        DatasetTable table = new DatasetTable(ds, "whatever");
        for (int i = 0; i < N; i++) {
          if (i == 0) {
            table.forEach(a -> basicTuples.add(a.toArray(new Object[0]))); // iterate only once because it is costly
            tuples.addAll(basicTuples);
          } else {
            final int j = i;
            basicTuples.forEach(a -> {
              Object[] copy = Arrays.copyOf(a, a.length);
              copy[keyIndex] = (long) a[keyIndex] + size * j;
              tuples.add(copy);
            });
          }
        }

        System.out.println("Loading scenario " + scenario + " ...");
        tm.load(scenario, ordersStore, tuples);
        System.out.println("Data for scenario " + scenario + " done");
        return tuples.size();
      };
    }
  }
}
