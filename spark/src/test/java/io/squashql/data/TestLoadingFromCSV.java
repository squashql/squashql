package io.squashql.data;

import io.squashql.SparkDatastore;
import io.squashql.transaction.SparkDataLoader;
import io.squashql.transaction.DataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

public class TestLoadingFromCSV {

  private static Function<String, Path> pathFunction = fileName -> {
    URL resource = Thread.currentThread().getContextClassLoader().getResource(fileName);
    try {
      if (resource == null) {
        throw new RuntimeException("Cannot find " + fileName);
      }
      URI uri = resource.toURI();
      return Paths.get(uri);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  };

  private static final String delimiter = ",";
  private static final boolean header = true;

  @Test
  void test() {
    String customersStore = "customersStore";
    String ordersStore = "ordersStore";
    SparkDatastore datastore = new SparkDatastore();
    SparkDataLoader tm = new SparkDataLoader(datastore.spark);

    tm.loadCsv(DataLoader.MAIN_SCENARIO_NAME, customersStore, pathFunction.apply("customers.csv").toString(), delimiter, header);
    tm.loadCsv(DataLoader.MAIN_SCENARIO_NAME, ordersStore, pathFunction.apply("orders.csv").toString(), delimiter, header);

    Dataset<Row> customersDS = datastore.get(customersStore);
    Dataset<Row> ordersDS = datastore.get(ordersStore);
    Assertions.assertThat(customersDS.count()).isEqualTo(91);
    Assertions.assertThat(customersDS.columns().length).isEqualTo(7 + 1); // +1 because of scenario
    Assertions.assertThat(ordersDS.count()).isEqualTo(196);
    Assertions.assertThat(ordersDS.columns().length).isEqualTo(5 + 1);  // +1 because of scenario
  }
}
