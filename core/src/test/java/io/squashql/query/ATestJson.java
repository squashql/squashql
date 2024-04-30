package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.jackson.JacksonUtil;
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
import org.postgresql.util.PGobject;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@TestClass(ignore = TestClass.Type.SNOWFLAKE)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestJson extends ABaseTestQuery {

  final String storeName = "mystore" + getClass().getSimpleName().toLowerCase();
  final Field ean = new TableField(this.storeName, "ean");
  final Field price = new TableField(this.storeName, "price");
  final Field priceConstruction = new TableField(this.storeName, "price_construction");

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    TableTypedField priceConstruction = new TableTypedField(this.storeName, "price_construction", Object.class); // JSON
    return Map.of(this.storeName, List.of(ean, price, priceConstruction));
  }

  @NoArgsConstructor
  @AllArgsConstructor
  private static class JsonObject {
    public boolean a;
    public String b;
    public int c;
  }

  public PGobject wrap(Object o) {
    PGobject pGobject = new PGobject();
    pGobject.setType("json");
    try {
      pGobject.setValue(JacksonUtil.serialize(o));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return pGobject;
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{"A", 10d, JacksonUtil.serialize(new JsonObject(true, "idA", 0))},
            new Object[]{"B", 8d, JacksonUtil.serialize(new JsonObject(true, "idB", 1))}
    ));
  }

  @Test
  void testQueryJson() {
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(this.ean), List.of(new AggregatedMeasure("json", this.priceConstruction, AggregationFunction.ANY_VALUE)))
            .build();
    Table table = this.executor.executeQuery(query);
    table.show();
    Assertions.assertThat(table).containsExactly(
            List.of("A", "{\"type\":\"json\",\"value\":null,\"null\":true}"),
            List.of("B", "{\"type\":\"json\",\"value\":null,\"null\":true}"));
  }
}
