package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Field;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static io.squashql.query.Functions.sum;
import static io.squashql.transaction.TransactionManager.MAIN_SCENARIO_NAME;

@TestClass(ignore = {TestClass.Type.SPARK, TestClass.Type.BIGQUERY, TestClass.Type.SNOWFLAKE})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestDocDrillacross extends ABaseTestQuery {

  @Override
  protected Map<String, List<Field>> getFieldsByStore() {
    Field ean = new Field("ean", String.class);
    Field qty = new Field("quantity", int.class);
    Field reason = new Field("reason", String.class);
    return Map.of(
            "shipment", List.of(ean, qty),
            "return", List.of(ean, qty, reason));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, "shipment", List.of(
            new Object[]{"A", 15},
            new Object[]{"B", 23},
            new Object[]{"C", 16}
    ));

    this.tm.load(MAIN_SCENARIO_NAME, "return", List.of(
            new Object[]{"A", 1, "defective"},
            new Object[]{"C", 3, "unwanted"}
    ));
  }

  @Test
  void test() {
    QueryDto query1 = Query
            .from("shipment")
            .select(List.of("ean"), List.of(sum("quantity sold", "quantity")))
//            .rollup(List.of("ean"))
            .build();

    QueryDto query2 = Query
            .from("return")
            .select(List.of("ean"), List.of(sum("quantity returned", "quantity")))
//            .rollup(List.of("ean"))
            .build();

    BiConsumer<QueryDto, QueryDto> runnable = (q1, q2) -> this.executor.execute(q1, q2, null).show();
    runnable.accept(query1, query2);

    query2 = Query
            .from("return")
            .select(List.of("ean", "reason"), List.of(sum("quantity returned", "quantity")))
            .rollup(List.of("reason"))
            .build();
    runnable.accept(query1, query2);
  }
}
