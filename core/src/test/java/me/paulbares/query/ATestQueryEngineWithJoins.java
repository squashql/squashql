package me.paulbares.query;

import me.paulbares.query.dto.JoinMappingDto;
import me.paulbares.query.database.DatabaseQuery;
import me.paulbares.query.dto.TableDto;
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

import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryEngineWithJoins {

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

  protected QueryEngine queryEngine;

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
    this.queryEngine = createQueryEngine(this.datastore);
    this.tm = createTransactionManager();

    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.orders, pathFunction.apply("orders.csv").toString(), delimiter, header);
    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.shippers, pathFunction.apply("shippers.csv").toString(), delimiter, header);
    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.products, pathFunction.apply("products.csv").toString(), delimiter, header);
    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.orderDetails, pathFunction.apply("order_details.csv").toString(), delimiter, header);
    this.tm.loadCsv(MAIN_SCENARIO_NAME, this.categories, pathFunction.apply("categories.csv").toString(), delimiter, header);
  }

  @Test
  void testQuerySingleCoordinate() {
    TableDto ordersTable = new TableDto(this.orders);
    TableDto orderDetailsTable = new TableDto(this.orderDetails);
    TableDto shippersTable = new TableDto(this.shippers);
    TableDto productsTable = new TableDto(this.products);
    TableDto categoriesTable = new TableDto(this.categories);

    ordersTable.join(orderDetailsTable, "inner", new JoinMappingDto("OrderID", "OrderID"));
    ordersTable.join(shippersTable, "inner", new JoinMappingDto("ShipperID", "ShipperID"));
    orderDetailsTable.join(productsTable, "inner", new JoinMappingDto("ProductID", "ProductID"));
    productsTable.join(categoriesTable, "inner", new JoinMappingDto("CategoryID", "CategoryID"));

    DatabaseQuery query = new DatabaseQuery()
            .table(ordersTable)
            .wildcardCoordinate("CategoryName")
            .aggregatedMeasure("Quantity", "sum")
            .aggregatedMeasure("*", "count");

    Table table = this.queryEngine.execute(query);
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
