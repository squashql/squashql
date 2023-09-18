package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.ConditionType;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import io.squashql.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.squashql.query.Functions.criterion;
import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestQueryWithJoins extends ABaseTestQuery {

  protected String storeName(String prefix) {
    return prefix + getClass().getSimpleName().toLowerCase();
  }

  protected final String orders = storeName("orders");
  protected final String orderDetails = storeName("orderDetails");
  protected final String shippers = storeName("shippers");
  protected final String products = storeName("products");
  protected final String categories = storeName("categories");

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    Function<String, TableTypedField> orderId = s -> new TableTypedField(s, "orderId", int.class);
    Function<String, TableTypedField> shipperId = s -> new TableTypedField(s, "shipperId", int.class);
    TableTypedField orderDetailsId = new TableTypedField(this.orderDetails, "orderDetailsId", int.class);
    Function<String, TableTypedField> productId = s -> new TableTypedField(s, "productId", int.class);
    TableTypedField quantity = new TableTypedField(this.orderDetails, "quantity", int.class);
    TableTypedField shipperName = new TableTypedField(this.shippers, "name", String.class);
    TableTypedField productName = new TableTypedField(this.products, "name", String.class);
    Function<String, TableTypedField> categoryId = s -> new TableTypedField(s, "categoryId", int.class);
    TableTypedField price = new TableTypedField(this.products, "price", double.class);
    TableTypedField categoryName = new TableTypedField(this.categories, "name", String.class);
    return Map.of(
            this.orders, List.of(orderId.apply(this.orders), shipperId.apply(this.orders)),
            this.orderDetails, List.of(orderDetailsId, orderId.apply(this.orderDetails), productId.apply(this.orderDetails), quantity),
            this.shippers, List.of(shipperId.apply(this.shippers), shipperName),
            this.products, List.of(productId.apply(this.products), productName, categoryId.apply(this.products), price),
            this.categories, List.of(categoryId.apply(this.categories), categoryName));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, this.orders, List.of(
            new Object[]{0, 100},
            new Object[]{1, 101},
            new Object[]{2, 102}
    ));
    this.tm.load(MAIN_SCENARIO_NAME, this.orderDetails, List.of(
            new Object[]{10, 0, 1001, 1},
            new Object[]{11, 0, 1002, 4},
            new Object[]{11, 0, 1003, 2},
            new Object[]{12, 1, 1004, 10},
            new Object[]{13, 1, 1005, 1},
            new Object[]{14, 2, 1006, 8}
    ));
    this.tm.load(MAIN_SCENARIO_NAME, this.shippers, List.of(
            new Object[]{100, "Speedy Express"},
            new Object[]{101, "United Package"},
            new Object[]{102, "Federal Shipping"}
    ));
    this.tm.load(MAIN_SCENARIO_NAME, this.products, List.of(
            new Object[]{1001, "Chang", 10_001, 18d},
            new Object[]{1002, "Aniseed Syrup", 10_002, 20d},
            new Object[]{1003, "Genen Shouyu", 10_002, 4d},
            new Object[]{1004, "Chocolade", 10_003, 5d},
            new Object[]{1005, "Pavlova", 10_003, 6d},
            new Object[]{1006, "Camembert Pierrot", 10_004, 20d}
    ));
    this.tm.load(MAIN_SCENARIO_NAME, this.categories, List.of(
            new Object[]{10_001, "Beverages"},
            new Object[]{10_002, "Condiments"},
            new Object[]{10_003, "Confections"},
            new Object[]{10_004, "Dairy Products"}
    ));
  }

  @Test
  void testSelectFullPath() {
    QueryDto query = Query
            .from(this.orders)
            .join(this.orderDetails, JoinType.INNER)
            .on(criterion(this.orderDetails + ".orderId", this.orders + ".orderId", ConditionType.EQ))
            .join(this.shippers, JoinType.INNER)
            .on(criterion(this.shippers + ".shipperId", this.orders + ".shipperId", ConditionType.EQ))
            .join(this.products, JoinType.INNER)
            .on(criterion(this.products + ".productId", this.orderDetails + ".productId", ConditionType.EQ))
            .join(this.categories, JoinType.INNER)
            .on(criterion(this.products + ".categoryId", this.categories + ".categoryId", ConditionType.EQ))
            // Select a field that exists in two tables: Products and Categories. If any ambiguity, it has to be solved
            // by the user by indicating the table from which the field should come from.
            .select(List.of(this.categories + ".name", this.products + ".name"), List.of(Functions.sum("quantity_sum", "quantity")))
            .build();

    Table table = this.executor.execute(query);
    Assertions.assertThat(table).containsExactly(
            List.of("Beverages", "Chang", 1l),
            List.of("Condiments", "Aniseed Syrup", 4l),
            List.of("Condiments", "Genen Shouyu", 2l),
            List.of("Confections", "Chocolade", 10l),
            List.of("Confections", "Pavlova", 1l),
            List.of("Dairy Products", "Camembert Pierrot", 8l));
    Assertions.assertThat(table.headers().stream().map(Header::name))
            .containsExactly(this.categories + ".name", this.products + ".name", "quantity_sum");
  }

  @Test
  void testAmbiguousColumnName() {
    QueryDto query = Query
            .from(this.orders)
            .join(this.orderDetails, JoinType.INNER)
            .on(criterion(this.orderDetails + ".orderId", this.orders + ".orderId", ConditionType.EQ))
            .join(this.shippers, JoinType.INNER)
            .on(criterion(this.shippers + ".shipperId", this.orders + ".shipperId", ConditionType.EQ))
            .join(this.products, JoinType.INNER)
            .on(criterion(this.products + ".productId", this.orderDetails + ".productId", ConditionType.EQ))
            .join(this.categories, JoinType.INNER)
            .on(criterion(this.products + ".categoryId", this.categories + ".categoryId", ConditionType.EQ))
            // Select a field that exists in two tables: Products and Categories. If any ambiguity, it has to be solved
            // by the user by indicating the table from which the field should come from.
            .select(List.of("name"), List.of(Functions.sum("quantity_sum", "quantity")))
            .build();
    TestUtil.assertThatThrownBy(() -> this.executor.execute(query)).hasMessageContaining(ambiguousNameMessage());
  }

  protected String ambiguousNameMessage() {
    String ds = this.datastore.getClass().getSimpleName();
    if (ds.contains(TestClass.Type.SPARK.className)) {
      return "Reference `name` is ambiguous";
    } else if (ds.contains(TestClass.Type.BIGQUERY.className)) {
      return "Column name name is ambiguous";
    } else if (ds.contains(TestClass.Type.CLICKHOUSE.className)) {
      return "Ambiguous column 'name'";
    } else if (ds.contains(TestClass.Type.SNOWFLAKE.className)) {
      return "ambiguous column name 'name'";
    } else if (ds.contains(TestClass.Type.DUCKDB.className)) {
      return "Ambiguous reference to column name \"name\"";
    }
    return null;
  }
}
