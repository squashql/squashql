package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.agg.AggregationFunction;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

@TestClass(ignore = {TestClass.Type.SNOWFLAKE, TestClass.Type.DUCKDB, TestClass.Type.SPARK, TestClass.Type.CLICKHOUSE})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestJson extends ABaseTestQuery {

  final String storeName = "mystore" + getClass().getSimpleName().toLowerCase();
  final Field ean = new TableField(this.storeName, "ean");
  final Field price = new TableField(this.storeName, "price");
  final Field priceConstruction = new TableField(this.storeName, "price_construction");
  final Field priceConstructionArray = new TableField(this.storeName, "price_construction_array");

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    TableTypedField priceConstruction = new TableTypedField(this.storeName, "price_construction", Object.class); // JSON
    TableTypedField priceConstructionArray = new TableTypedField(this.storeName, "price_construction_array", Object.class); // JSON
    return Map.of(this.storeName, List.of(ean, price, priceConstruction, priceConstructionArray));
  }

  @NoArgsConstructor
  @AllArgsConstructor
  private static class JsonObject {
    public boolean a;
    public String b;
    public int c;
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{"A", 10d, new JsonObject(true, "idA", 0), List.of(new JsonObject(true, "idA", 10), new JsonObject(true, "idA", 11))},
            new Object[]{"B", 8d, new JsonObject(true, "idB", 1), List.of(new JsonObject(false, "idB", 10), new JsonObject(false, "idB", 11))}
    ));
  }

  @Test
  void testQueryJson() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(this.ean), List.of(
                    new AggregatedMeasure("json", this.priceConstruction, AggregationFunction.ANY_VALUE),
                    new AggregatedMeasure("jsonArray", this.priceConstructionArray, AggregationFunction.ANY_VALUE)
            ))
            .build();
    Table table = this.executor.executeQuery(query);
    table.show();
    // '{"a": {"b":"foo"}}'::json->'a'
    // '[1,2,3]'::json->>2
//    this.executor.executeRaw("SELECT JSON_VALUE(JSON '{\"name\": \"Jakob\", \"age\": \"6\" }', '$.age') from " + this.executor.queryEngine.queryRewriter().tableName(this.storeName));
//    this.executor.executeRaw("select price_construction::json->>'a' from " + this.executor.queryEngine.queryRewriter().tableName(this.storeName)).show();
    Assertions.assertThat(table).containsExactly(
            List.of("A", "{\"a\":true,\"b\":\"idA\",\"c\":0}", "[{\"a\":true,\"b\":\"idA\",\"c\":10},{\"a\":true,\"b\":\"idA\",\"c\":11}]"),
            List.of("B", "{\"a\":true,\"b\":\"idB\",\"c\":1}", "[{\"a\":false,\"b\":\"idB\",\"c\":10},{\"a\":false,\"b\":\"idB\",\"c\":11}]"));
  }
}
