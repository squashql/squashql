package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.dto.ConditionType;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.VirtualTableDto;
import io.squashql.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.squashql.query.Functions.all;
import static io.squashql.query.Functions.criterion;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestBucketing extends ABaseTestQuery {

  protected String storeName = "store" + getClass().getSimpleName().toLowerCase();

  @Override
  protected Map<String, List<Field>> getFieldsByStore() {
    Field ean = new Field(this.storeName, "ean", int.class);
    Field shop = new Field(this.storeName, "shop", int.class);
    Field unitPrice = new Field(this.storeName, "unitPrice", double.class);
    Field qtySold = new Field(this.storeName, "qtySold", int.class);
    Field kvi = new Field(this.storeName, "kvi", double.class);
    return Map.of(this.storeName, List.of(ean, shop, unitPrice, qtySold, kvi));
  }

  @Override
  protected void loadData() {
    List<Object[]> tuples = new ArrayList<>();
    for (int shop = 0; shop < 2; shop++) {
      for (int ean = 0; ean < 10; ean++) {
        tuples.add(new Object[]{ean, shop, (double) ean, 10, (ean + 1) * 10d});
      }
    }
    this.tm.load(this.storeName, tuples);
  }

  @Test
  void test() {
    // TODO to delete
//    SELECT arrayJoin([
//            tuple(1, 'A'),
//            tuple(2, 'B'),
//            tuple(3, 'C')
//            ]) AS res
    // arrayJoin: like unnest or explode
//    Table execute = this.executor.execute("""
//            SELECT 1, 'A'
//            UNION ALL
//            SELECT 2, 'B'
//            AS res
//            """);
//    Table execute = this.executor.execute("""
//            WITH res as (
//            SELECT 1 as first, 'A' as second
//            UNION ALL
//            SELECT 2 as first, 'B' as second
//            )
//            SELECT res.first, res.second from res
//            """);
//        select "shop", "MYTEMPTABLE"."bucket", sum("unitPrice" * "qtySold") as "sales", count(*) as "_contributors_count_"

    // FOR SNOWFLAKE, do not escape anything: "MYTEMPTABLE"."bucket" is not supported but MYTEMPTABLE.bucket is ok.
    // FOR BIGQUERY, do not use full path with dataset name for CTE....
//    Table execute = this.executor.execute("""
//        with "MYTEMPTABLE" as (
//        select 'unsensistive' as "bucket",  0.0 as "MYTEMPTABLE_min",  50.0 as "MYTEMPTABLE_max"
//        union all
//        select 'sensistive' as "bucket",  50.0 as "MYTEMPTABLE_min",  80.0 as "MYTEMPTABLE_max"
//        union all
//        select 'hypersensistive' as "bucket",  80.0 as "MYTEMPTABLE_min",  100.0 as "MYTEMPTABLE_max"
//        )
//        select "shop", "MYTEMPTABLE"."bucket", sum("unitPrice" * "qtySold") as "sales", count(*) as "_contributors_count_"
//        from "storetestsnowflakebucketing"
//        inner join "MYTEMPTABLE"
//        on
//        "storetestsnowflakebucketing"."kvi" >= "MYTEMPTABLE"."MYTEMPTABLE_min"
//        and
//        "storetestsnowflakebucketing"."kvi" < "MYTEMPTABLE"."MYTEMPTABLE_max"
//        group by "shop", "MYTEMPTABLE"."bucket" limit 10000
//            """);
//    execute.show();
  }

  @Test
  void testSimpleFieldName() {
    test("bucket");
  }

  @Test
  void testFullFieldName() {
    test("sensitivities.bucket"); // this syntax should be supported
  }

  void test(String bucketFieldName) {
    QueryRewriter qr = this.executor.queryEngine.queryRewriter();
    String expression = String.format("sum(%s * %s)", qr.fieldName("unitPrice"), qr.fieldName("qtySold"));
    ExpressionMeasure sales = new ExpressionMeasure("sales", expression);

    CriteriaDto criteria = all(criterion("kvi", "min", ConditionType.GE), criterion("kvi", "max", ConditionType.LT));
    VirtualTableDto sensitivities = new VirtualTableDto("sensitivities", List.of("bucket", "min", "max"), List.of(
            List.of("unsensistive", 0d, 50d),
            List.of("sensistive", 50d, 80d),
            List.of("hypersensistive", 80d, 101d)
    ));
    var query = Query
            .from(this.storeName)
            .innerJoin(sensitivities)
            .on(criteria)
            .select(List.of("shop", bucketFieldName), List.of(sales))
            .build();

    Table result = this.executor.execute(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly("shop", bucketFieldName, "sales");
    Assertions.assertThat(result).containsExactly(
            List.of(0, "hypersensistive", 240d),
            List.of(0, "sensistive", 150d),
            List.of(0, "unsensistive", 60d),
            List.of(1, "hypersensistive", 240d),
            List.of(1, "sensistive", 150d),
            List.of(1, "unsensistive", 60d));

    query = Query
            .from(this.storeName)
            .innerJoin(sensitivities)
            .on(criteria)
            .select(List.of("shop", bucketFieldName), List.of(sales))
            .rollup("shop", bucketFieldName)
            .build();
    result = this.executor.execute(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly("shop", bucketFieldName, "sales");
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, 900d),
            List.of(0, TOTAL, 450d),
            List.of(0, "hypersensistive", 240d),
            List.of(0, "sensistive", 150d),
            List.of(0, "unsensistive", 60d),
            List.of(1, TOTAL, 450d),
            List.of(1, "hypersensistive", 240d),
            List.of(1, "sensistive", 150d),
            List.of(1, "unsensistive", 60d));
  }
}
