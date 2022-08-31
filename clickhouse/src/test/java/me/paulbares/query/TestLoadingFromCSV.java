package me.paulbares.query;

import me.paulbares.ClickHouseDatastore;
import me.paulbares.query.database.ClickHouseQueryEngine;
import me.paulbares.store.Field;
import me.paulbares.transaction.ClickHouseTransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;

import static me.paulbares.query.TestUtils.createClickHouseContainer;
import static me.paulbares.query.TestUtils.jdbcUrl;
import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static me.paulbares.transaction.TransactionManager.SCENARIO_FIELD_NAME;

@Testcontainers
public class TestLoadingFromCSV {

  @Container
  public GenericContainer container = createClickHouseContainer();

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
    ClickHouseDatastore datastore = new ClickHouseDatastore(jdbcUrl.apply(container), null);
    ClickHouseTransactionManager tm = new ClickHouseTransactionManager(datastore.getDataSource());

    String storeName = "myAwesomeStore";
    tm.dropAndCreateInMemoryTable(storeName, List.of(
            new Field("CustomerID", int.class),
            new Field("CustomerName", String.class),
            new Field("ContactName", String.class),
            new Field("Address", String.class),
            new Field("City", String.class),
            new Field("PostalCode", String.class),
            new Field("Country", String.class)
    ));

    tm.loadCsv(MAIN_SCENARIO_NAME, storeName, pathFunction.apply("customers.csv").toString(), delimiter, header);

    ClickHouseQueryEngine engine = new ClickHouseQueryEngine(datastore);
    Table table = new QueryExecutor(engine)
            .execute(QueryBuilder.query()
                    .table(storeName)
                    .withColumn(SCENARIO_FIELD_NAME)
                    .aggregatedMeasure("count", "*", "count"));
    Assertions.assertThat(table).containsExactlyInAnyOrder(List.of("", 91L)); // empty string because no scenario in
    // csv file
  }
}
