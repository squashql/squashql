package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.BucketColumnSetDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Field;
import io.squashql.table.Table;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;
import static io.squashql.transaction.DataLoader.SCENARIO_FIELD_NAME;

/**
 * This test class is used to verify and print tables for the documentation. Nothing is asserted in those tests this is
 * why it is @{@link Disabled}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestClass(ignore = {TestClass.Type.SPARK, TestClass.Type.BIGQUERY, TestClass.Type.SNOWFLAKE, TestClass.Type.CLICKHOUSE})
@Disabled
public abstract class ATestDocBucketComparison extends ABaseTestQuery {

  @Override
  protected Map<String, List<Field>> getFieldsByStore() {
    Field salePrice = new Field("store", "saleprice", double.class);
    Field loavesSold = new Field("store", "loavessold", int.class);
    Field pos = new Field("store", "pointofsale", String.class);
    return Map.of("store", List.of(salePrice, loavesSold, pos));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, "store", List.of(new Object[]{2d, 100, "A"}, new Object[]{2d, 80, "B"}));
    this.tm.load("s1", "store", List.of(new Object[]{3d, 74, "A"}, new Object[]{3d, 50, "B"}));
    this.tm.load("s2", "store", List.of(new Object[]{4d, 55, "A"}, new Object[]{4d, 20, "B"}));
    this.tm.load("s3", "store", List.of(new Object[]{2d, 100, "A"}, new Object[]{3d, 50, "B"}));
  }

  @Test
  void test() {
    Measure revenue = new ExpressionMeasure("revenue", "sum(saleprice * loavessold)");
    BucketColumnSetDto bucketCS = new BucketColumnSetDto("group", SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "s1"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "s2"))
            .withNewBucket("group3", List.of(MAIN_SCENARIO_NAME, "s3"))
            .withNewBucket("group4", List.of(MAIN_SCENARIO_NAME, "s1", "s2", "s3"));
    ComparisonMeasureReferencePosition revenueComparison = new ComparisonMeasureReferencePosition(
            "revenueComparison",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            revenue,
            Map.of(SCENARIO_FIELD_NAME, "s-1", "group", "g"),
            ColumnSetKey.BUCKET);

    QueryDto queryDto = Query.from("store")
//            .select(List.of(SCENARIO_FIELD_NAME, "saleprice", "loavessold", "pointofsale"),List.of(CountMeasure.INSTANCE, revenue))
            .select(List.of(), List.of(bucketCS), List.of(CountMeasure.INSTANCE, revenue, revenueComparison))
            .build();
    Table result = this.executor.execute(queryDto);
    result.show();
  }
}
