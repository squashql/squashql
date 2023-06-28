package io.squashql.query;

import io.squashql.query.builder.Query;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.dto.ConditionType;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Datastore;
import io.squashql.table.Table;
import io.squashql.transaction.DataLoader;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;

import static io.squashql.query.Functions.criterion;
import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryExecutorWithJoins {

  private static Function<String, Path> pathFunction = fileName -> {
    URL resource = Thread.currentThread().getContextClassLoader().getResource(fileName);
    try {
      if (resource == null) {
        throw new RuntimeException("Cannot find " + fileName);
      }
      return Paths.get(resource.toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  };

  private static final String delimiter = ",";
  private static final boolean header = true;

  protected Datastore datastore;

  protected DataLoader tm;

  protected QueryExecutor queryExecutor;

  protected String orders = "orders";
  protected String orderDetails = "orderDetails";
  protected String shippers = "shippers";
  protected String products = "products";
  protected String categories = "categories";

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore();

  protected abstract DataLoader createDataLoader();

  @BeforeAll
  void setup() {
    this.datastore = createDatastore();
    this.tm = createDataLoader();

    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.orders, pathFunction.apply("orders.csv").toString(), delimiter, header);
    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.shippers, pathFunction.apply("shippers.csv").toString(), delimiter, header);
    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.products, pathFunction.apply("products.csv").toString(), delimiter, header);
    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.orderDetails, pathFunction.apply("order_details.csv").toString(), delimiter, header);
    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.categories, pathFunction.apply("categories.csv").toString(), delimiter, header);

    this.queryExecutor = new QueryExecutor(createQueryEngine(this.datastore));
  }

  @Test
  void testQuerySingleCoordinate() {
    QueryDto query = Query
            .from(this.orders)
            .join(this.orderDetails, JoinType.INNER)
            .on(criterion(this.orderDetails + ".OrderID", this.orders + ".OrderID", ConditionType.EQ))
            .join(this.shippers, JoinType.INNER)
            .on(criterion(this.shippers + ".ShipperID", this.orders + ".ShipperID", ConditionType.EQ))
            .join(this.products, JoinType.INNER)
            .on(criterion(this.products + ".ProductID", this.orderDetails + ".ProductID", ConditionType.EQ))
            .join(this.categories, JoinType.INNER)
            .on(criterion(this.products + ".CategoryID", this.categories + ".CategoryID", ConditionType.EQ))
            .select(List.of("CategoryName"), List.of(Functions.sum("Q", "Quantity"), CountMeasure.INSTANCE))
            .build();

    Table table = this.queryExecutor.execute(query);
    Assertions.assertThat(table).containsExactlyInAnyOrder(
            List.of("Dairy Products", 2601.0, 100L),
            List.of("Meat/Poultry", 1288.0, 50L),
            List.of("Condiments", 1383.0, 49L),
            List.of("Beverages", 2289.0, 93L),
            List.of("Grains/Cereals", 912.0, 42L),
            List.of("Seafood", 1445.0, 67L),
            List.of("Confections", 2110.0, 84L),
            List.of("Produce", 715.0, 33L));
  }
}
