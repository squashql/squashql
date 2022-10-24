package me.paulbares.query;

import me.paulbares.query.builder.QueryBuilder2;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.transaction.TransactionManager;
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

import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;

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

  protected TransactionManager tm;

  protected QueryExecutor queryExecutor;

  protected String orders = "orders";
  protected String orderDetails = "orderDetails";
  protected String shippers = "shippers";
  protected String products = "products";
  protected String categories = "categories";

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore();

  protected abstract TransactionManager createTransactionManager();

  @BeforeAll
  void setup() {
    this.datastore = createDatastore();
    this.queryExecutor = new QueryExecutor(createQueryEngine(this.datastore));
    this.tm = createTransactionManager();

    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.orders, pathFunction.apply("orders.csv").toString(), delimiter, header);
    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.shippers, pathFunction.apply("shippers.csv").toString(), delimiter, header);
    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.products, pathFunction.apply("products.csv").toString(), delimiter, header);
    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.orderDetails, pathFunction.apply("order_details.csv").toString(), delimiter, header);
    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.categories, pathFunction.apply("categories.csv").toString(), delimiter, header);
  }

  @Test
  void testQuerySingleCoordinate() {
    QueryDto queryDto = QueryBuilder2
            .from(this.orders)
            .inner_join(this.orderDetails)
            .on(this.orderDetails, "OrderID", this.orders, "OrderID")
            .inner_join(this.shippers)
            .on(this.shippers, "ShipperID", this.orders, "ShipperID")
            .inner_join(this.products)
            .on(this.products, "ProductID", this.orderDetails, "ProductID")
            .inner_join(this.categories)
            .on(this.products, "CategoryID", this.categories, "CategoryID")
            .select(List.of("CategoryName"), List.of(QueryBuilder.sum("Q", "Quantity"), CountMeasure.INSTANCE))
            .build();

    Table table = this.queryExecutor.execute(queryDto);
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
