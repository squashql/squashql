package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.list.Lists;
import io.squashql.type.TableTypedField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

@TestClass(ignore = TestClass.Type.SNOWFLAKE)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestVector extends ABaseTestQuery {
  final String storeName = "mystore";// + getClass().getSimpleName().toLowerCase();
  final Field competitor = new TableField(this.storeName, "competitor");
  final Field ean = new TableField(this.storeName, "ean");
  final Field price = new TableField(this.storeName, "price");

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    TableTypedField tag = new TableTypedField(this.storeName, "tag", Lists.LongList.class);
    return Map.of(this.storeName, List.of(ean, price, tag));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{"A", 10d, List.of(1L, 3L, 5L)},
            new Object[]{"B", 8d, List.of(1L, 2L)},
            new Object[]{"C", 2d, List.of(2L, 4L, 6L)},
            new Object[]{"D", 1d, List.of(1L)}
    ));
  }

  @Test
  void test() {
//    this.executor.executeRaw("select * from " + this.storeName).show();
//    QueryDto query = Query
//            .from(this.storeName)
////            .where(criterion(Functions.integer(1), in()))
//            .select(List.of(this.ean), List.of(sum("price", this.price)))
//            .build();
//
//    this.executor.executeQuery(query)
//            .show();

    // Clickhouse
    //    this.executor.executeRaw("select * from mystore where has(tag, 2)").show();

    // DUCKDB, SPARK
    //    this.executor.executeRaw("select * from mystore where array_contains(tag, 2)").show();

    // BigQuery
//    this.executor.executeRaw("select * from "
//            + this.executor.queryEngine.queryRewriter(null).tableName(this.storeName)
//            + " where 2 in unnest(tag)").show();
  }
}
