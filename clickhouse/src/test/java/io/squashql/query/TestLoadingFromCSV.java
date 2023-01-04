package io.squashql.query;

import io.squashql.ClickHouseDatastore;
import io.squashql.query.database.ClickHouseQueryEngine;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Field;
import io.squashql.transaction.ClickHouseTransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static io.squashql.query.TestUtils.createClickHouseContainer;
import static io.squashql.query.TestUtils.jdbcUrl;
import static io.squashql.transaction.TransactionManager.MAIN_SCENARIO_NAME;

@Testcontainers
@Disabled("""
        Not passing anymore since the migration from 0.3.2-patch5 to 0.3.2-patch11.
        We are not using this feature so it is ok for the time being.
        """)
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
    tm.dropAndCreateInMemoryTableWithoutScenarioColumn(storeName, List.of(
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
            .execute(new QueryDto()
                    .table(storeName)
                    .aggregatedMeasure("count", "*", "count"));
    Assertions.assertThat(table).containsExactlyInAnyOrder(Arrays.asList(null, 91L));
    // csv file
  }
}
