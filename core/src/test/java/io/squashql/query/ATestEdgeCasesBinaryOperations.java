package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.squashql.query.Functions.divide;
import static io.squashql.query.Functions.sum;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestEdgeCasesBinaryOperations extends ABaseTestQuery {

  private final String storeName = "store" + getClass().getSimpleName().toLowerCase();
  private final Field ean = new TableField(this.storeName, "ean");
  private final Field price = new TableField(this.storeName, "price");
  private final Field qty = new TableField(this.storeName, "quantity");

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    TableTypedField ean = new TableTypedField(this.storeName, "ean", String.class);
    TableTypedField price = new TableTypedField(this.storeName, "price", double.class);
    TableTypedField qty = new TableTypedField(this.storeName, "quantity", int.class);
    return Map.of(this.storeName, List.of(ean, price, qty));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.storeName, List.of(
            new Object[]{"0", 2d, 10},
            new Object[]{"1", 3d, 20},
            new Object[]{"2", 0d, 0}
    ));
  }

  @Test
  void testDivideByZero() {
    Measure sum = sum("p", this.price);
    Field divide = divide(this.price, this.price);
    Measure avg = Functions.avg("avg", divide);
    QueryDto query = Query
            .from(this.storeName)
            .select(List.of(this.ean), List.of(divide("divide", sum, sum), avg))
            .build();
    Table result = this.executor.executeQuery(query);
    result.show();
    Assertions.assertThat(result).containsExactly(
            List.of("0", 1d, 1d),
            List.of("1", 1d, 1d),
            Arrays.asList("2", getReturnedValueDivisionByZero(), getReturnedValueDivisionByZero()));
  }

  /**
   * The returned value if an error occurs, such as a division by zero error.
   */
  private Object getReturnedValueDivisionByZero() {
    String lc = this.executor.queryEngine.getClass().getSimpleName().toLowerCase();
    if (lc.contains(TestClass.Type.SPARK.name().toLowerCase())
            || lc.contains(TestClass.Type.BIGQUERY.name().toLowerCase())) {
      return null;
    } else if (lc.contains(TestClass.Type.DUCKDB.name().toLowerCase())
            || lc.contains(TestClass.Type.CLICKHOUSE.name().toLowerCase())) {
      return Double.NaN;
    }  else if (lc.contains(TestClass.Type.SNOWFLAKE.name().toLowerCase())) {
      return 0d;
    } else {
      return null;
    }
  }
}
