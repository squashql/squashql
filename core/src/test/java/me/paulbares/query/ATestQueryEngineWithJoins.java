package me.paulbares.query;

import me.paulbares.query.dto.JoinMappingDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.TableDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Store;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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

  protected QueryEngine queryEngine;

  protected String orders = "orders";
  protected String orderDetails = "orderDetails";
  protected String shippers = "shippers";
  protected String products = "products";
  protected String categories = "categories";

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore(List<Store> stores);

  protected abstract Store createStore(String storeName);

  @BeforeAll
  void setup() {
    List<Store> stores = new ArrayList<>();

    stores.add(createStore(this.orders));
    stores.add(createStore(this.orderDetails));
    stores.add(createStore(this.products));
    stores.add(createStore(this.shippers));
    stores.add(createStore(this.categories));

    this.datastore = createDatastore(stores);
    this.queryEngine = createQueryEngine(this.datastore);

    this.datastore.loadCsv(MAIN_SCENARIO_NAME, this.orders, pathFunction.apply("orders.csv").toString(), delimiter, header);
    this.datastore.loadCsv(MAIN_SCENARIO_NAME, this.shippers, pathFunction.apply("shippers.csv").toString(), delimiter, header);
    this.datastore.loadCsv(MAIN_SCENARIO_NAME, this.products, pathFunction.apply("products.csv").toString(), delimiter, header);
    this.datastore.loadCsv(MAIN_SCENARIO_NAME, this.orderDetails, pathFunction.apply("order_details.csv").toString(), delimiter, header);
    this.datastore.loadCsv(MAIN_SCENARIO_NAME, this.categories, pathFunction.apply("categories.csv").toString(), delimiter, header);
  }

  @Test
  void testQuerySingleCoordinate() {
    TableDto ordersTable = new TableDto(this.orders);
    TableDto orderDetailsTable = new TableDto(this.orderDetails);
    TableDto shippersTable = new TableDto(this.shippers);
    TableDto productsTable = new TableDto(this.products);
    TableDto categoriesTable = new TableDto(this.categories);

    ordersTable.join(orderDetailsTable, "inner", new JoinMappingDto("OrderId", "OrderId"));
    ordersTable.join(shippersTable, "inner", new JoinMappingDto("ShipperId", "ShipperId"));
    orderDetailsTable.join(productsTable, "inner", new JoinMappingDto("ProductId", "ProductId"));
    productsTable.join(categoriesTable, "inner", new JoinMappingDto("CategoryID", "CategoryID"));

    QueryDto query = new QueryDto()
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