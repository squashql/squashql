package me.paulbares.benchmark;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import me.paulbares.SparkDatastore;
import me.paulbares.SparkUtil;
import me.paulbares.query.DatasetTable;
import me.paulbares.query.SparkQueryEngine;
import me.paulbares.query.Table;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.store.Store;
import me.paulbares.transaction.SparkTransactionManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

public class SparkEmbeddedQueryBenchmark {

  static {
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.INFO);
    Logger org = (Logger) LoggerFactory.getLogger("org");
    org.setLevel(Level.WARN);
  }

  private static final String delimiter = ",";
  private static final boolean header = true;
  private static String ordersStore = "ordersStore";
//  private static StorageLevel storageLevel = StorageLevel.MEMORY_AND_DISK();
//  private static StorageLevel storageLevel = StorageLevel.MEMORY_ONLY();
  private static int N = 2;

  public static void main(String[] args) throws Exception {
    Pair<SparkDatastore, DatastoreInfo> datastore = createAndLoadStore();
    SparkQueryEngine queryEngine = new SparkQueryEngine(datastore.getOne());
    List<Table> results = new ArrayList<>();
    BenchmarkRunner.SINGLE.run(() -> {
      QueryDto query = new QueryDto().table(ordersStore)
              .wildcardCoordinate(SCENARIO_FIELD_NAME)
              .coordinates("CategoryName", "Condiments", "Beverages")
              .aggregatedMeasure("price", "sum")
              .aggregatedMeasure("quantity", "sum");
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

  static Pair<SparkDatastore, DatastoreInfo> createAndLoadStore() {
    Store sparkStore = new Store(ordersStore, List.of(
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
            new Field("ShipperName", String.class)));
    SparkDatastore datastore = new SparkDatastore();
    SparkTransactionManager tm = new SparkTransactionManager(datastore.spark);

    String path = "spark/src/test/resources/benchmark/data_%s_scenario.csv";
    Function<String, String> pathFunction = scenario -> String.format(path, scenario);

    AtomicInteger count = new AtomicInteger();
    Consumer<String> loader = scenario -> {
      Dataset<Row> ds =
              datastore.spark.read()
                      .option("delimiter", delimiter)
                      .option("header", header)
                      // Use the schema to have tuples correctly formed otherwise all elements are strings
                      .schema(SparkUtil.createSchema(sparkStore.fields()))
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

      count.set(tuples.size());
      System.out.println("Loading scenario " + scenario + " ...");
      tm.load(scenario, ordersStore, tuples);
      System.out.println("Data for scenario " + scenario + " done");
    };

    List<String> scenarios = List.of(Datastore.MAIN_SCENARIO_NAME, "s50", "s25", "s10", "s05");
    scenarios.forEach(loader::accept);

//    datastore.persist(storageLevel);
    return Tuples.pair(datastore, new DatastoreInfo(count.get(), scenarios));
  }

  record DatastoreInfo(int nbLines, List<String> scenarios) {}
}
