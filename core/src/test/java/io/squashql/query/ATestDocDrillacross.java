package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.jackson.JacksonUtil;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.*;
import io.squashql.table.PivotTable;
import io.squashql.table.PivotTableUtils;
import io.squashql.type.TableTypedField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static io.squashql.query.Functions.sum;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestClass(ignore = {TestClass.Type.SPARK, TestClass.Type.BIGQUERY, TestClass.Type.SNOWFLAKE, TestClass.Type.CLICKHOUSE})
public abstract class ATestDocDrillacross extends ABaseTestQuery {

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField eanShipment = new TableTypedField("shipment", "product", String.class);
    TableTypedField eanReturn = new TableTypedField("return", "product", String.class);
    TableTypedField qtyShipment = new TableTypedField("shipment", "quantity", int.class);
    TableTypedField qtyReturn = new TableTypedField("return", "quantity", int.class);
    TableTypedField reason = new TableTypedField("return", "reason", String.class);
    return Map.of(
            "shipment", List.of(eanShipment, qtyShipment),
            "return", List.of(eanReturn, qtyReturn, reason));
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
            new Object[]{"C", 3, "unwanted"},
            new Object[]{"D", 1, "unwanted"}
    ));
  }

  @Test
  void test() {
    QueryDto query1 = Query
            .from("shipment")
            .select(tableFields(List.of("product")), List.of(sum("quantity sold", "quantity")))
//            .rollup(tableFields(List.of("product")))
            .build();

    QueryDto query2 = Query
            .from("return")
            .select(tableFields(List.of("product", "reason")), List.of(sum("quantity returned", "quantity")))
//            .rollup(tableFields(List.of("product", "reason")))
            .build();

    QueryMergeDto queryMerge = QueryMergeDto.from(query1).join(query2, JoinType.FULL);
    this.executor.executeQueryMerge(queryMerge, null).show();
    PivotTable pt = this.executor.executePivotQueryMerge(new PivotTableQueryMergeDto(QueryMergeDto.from(query1).join(query2, JoinType.FULL),
                    List.of(tableField("product")),
                    List.of(tableField("reason"))),
            null);
    pt.show();
    List<Map<String, Object>> cells = PivotTableUtils.generateCells(pt, false);
    System.out.println(JacksonUtil.serialize(new PivotTableQueryResultDto(cells, pt.rows, pt.columns, pt.values, pt.hideTotals)));
  }
}
