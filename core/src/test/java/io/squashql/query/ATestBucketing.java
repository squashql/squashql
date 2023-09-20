package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.ConditionType;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.VirtualTableDto;
import io.squashql.type.TableTypedField;
import io.squashql.table.Table;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static io.squashql.query.Functions.all;
import static io.squashql.query.Functions.criterion;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;

@TestClass(ignore = TestClass.Type.CLICKHOUSE)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestBucketing extends ABaseTestQuery {

  protected String storeName = "store" + getClass().getSimpleName().toLowerCase();

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ean = new TableTypedField(this.storeName, "ean", int.class);
    TableTypedField shop = new TableTypedField(this.storeName, "shop", String.class);
    TableTypedField unitPrice = new TableTypedField(this.storeName, "unitPrice", double.class);
    TableTypedField qtySold = new TableTypedField(this.storeName, "qtySold", int.class);
    TableTypedField kvi = new TableTypedField(this.storeName, "kvi", double.class);
    return Map.of(this.storeName, List.of(ean, shop, unitPrice, qtySold, kvi));
  }

  @Override
  protected void loadData() {
    List<Object[]> tuples = new ArrayList<>();
    for (int shop = 0; shop < 2; shop++) {
      for (int ean = 0; ean < 10; ean++) {
        tuples.add(new Object[]{ean, String.valueOf(shop), (double) ean, 10, (ean + 1) * 10d});
      }
    }
    this.tm.load(this.storeName, tuples);
  }

  static VirtualTableDto sensitivities = new VirtualTableDto("sensitivities", List.of("bucket", "min", "max"), List.of(
          List.of("unsensistive", 0d, 50d),
          List.of("sensistive", 50d, 80d),
          List.of("hypersensistive", 80d, 101d)
  ));

  @Test
  void testSimpleFieldName() {
    test(false);
  }

  @Test
  void testFullFieldName() {
    test(true);
  }

  void test(boolean useFullName) {
    QueryRewriter qr = this.executor.queryEngine.queryRewriter();
    String expression = String.format("sum(%s * %s)", qr.fieldName("unitPrice"), qr.fieldName("qtySold"));
    ExpressionMeasure sales = new ExpressionMeasure("sales", expression);
    BiFunction<String, String, String> fieldNameGenerator = (table, field) -> useFullName ? SqlUtils.getFieldFullName(table, field) : field;
    CriteriaDto criteria = all(
            criterion(fieldNameGenerator.apply(this.storeName, "kvi"), fieldNameGenerator.apply(sensitivities.name, "min"), ConditionType.GE),
            criterion(fieldNameGenerator.apply(this.storeName, "kvi"), fieldNameGenerator.apply(sensitivities.name, "max"), ConditionType.LT));
    String bucket = fieldNameGenerator.apply(sensitivities.name, "bucket");
    String shop = fieldNameGenerator.apply(this.storeName, "shop");
    var query = Query
            .from(this.storeName)
            .join(sensitivities, JoinType.INNER)
            .on(criteria)
            .select(List.of(shop, bucket), List.of(sales))
            .build();

    Table result = this.executor.execute(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(shop, bucket, "sales");
    Assertions.assertThat(result).containsExactly(
            List.of("0", "hypersensistive", 240d),
            List.of("0", "sensistive", 150d),
            List.of("0", "unsensistive", 60d),
            List.of("1", "hypersensistive", 240d),
            List.of("1", "sensistive", 150d),
            List.of("1", "unsensistive", 60d));

    query = Query
            .from(this.storeName)
            .join(sensitivities, JoinType.INNER)
            .on(criteria)
            .select(List.of(shop, bucket), List.of(sales))
            .rollup(shop, bucket)
            .build();
    result = this.executor.execute(query);
    Assertions.assertThat(result.headers().stream().map(Header::name))
            .containsExactly(shop, bucket, "sales");
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, GRAND_TOTAL, 900d),
            List.of("0", TOTAL, 450d),
            List.of("0", "hypersensistive", 240d),
            List.of("0", "sensistive", 150d),
            List.of("0", "unsensistive", 60d),
            List.of("1", TOTAL, 450d),
            List.of("1", "hypersensistive", 240d),
            List.of("1", "sensistive", 150d),
            List.of("1", "unsensistive", 60d));
  }
}
