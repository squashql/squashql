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

  // Conditions

  // 1. where (orderDate > 1/12/1996 AND orderDate < 31/12/1996) OR (orderDate > 1/10/1996 AND orderDate < 31/10/1996)
  // 2. where category in (Condiments, Dairy) <=> category eq Condiments OR category eq Dairy
  // 3. where category not in (Condiments, Dairy) <=> category neq Condiments AND category neq Dairy
  // 4. where category eq "Dairy"
  // 5. where category neq "Dairy"
  // 6. where cond1 and cond2 and cond3
  // 6. where cond1 or cond2 or cond3

  // https://stackoverflow.com/questions/20737045/representing-logic-as-data-in-json
  /* If you must implement this using standard JSON, I'd recommend something akin to
   *Lisp's "S-expressions". A condition could be either a plain object, or an array whose
   *first entry is the logical operation that joins them.
   * ["AND",
    {"var1" : "value1"},
    ["OR",
        { "var2" : "value2" },
        { "var3" : "value3" }
    ]
]
* var1 == value1 AND (var2 == value2 OR var3 == value3)
   */
}
