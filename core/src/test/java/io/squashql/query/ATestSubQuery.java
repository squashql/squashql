package io.squashql.query;

import static io.squashql.query.Functions.avg;
import static io.squashql.query.Functions.criterion;
import static io.squashql.query.Functions.ge;
import static io.squashql.query.Functions.min;
import static io.squashql.query.Functions.sum;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.agg.AggregationFunction.AVG;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.QueryDto;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestSubQuery extends ABaseTestQuery {

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    // See https://mariadb.com/kb/en/subqueries-in-a-from-clause/
    TableTypedField studentName = new TableTypedField("student", "name", String.class);
    TableTypedField test = new TableTypedField("student", "test", String.class);
    TableTypedField score = new TableTypedField("student", "score", int.class);
    return Map.of("student", List.of(studentName, test, score));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, "student", List.of(
            new Object[]{"Paul", "sql", 75},
            new Object[]{"Paul", "java", 73},
            new Object[]{"Peter", "sql", 43},
            new Object[]{"Peter", "java", 33},
            new Object[]{"Tatiana", "sql", 87},
            new Object[]{"Tatiana", "java", 85}
    ));
  }

  @Test
  void testSubQuery() {
    QueryDto subQuery = Query.from("student")
            .select(tableFields(List.of("name")), List.of(sum("score_sum", "score")))
            .build();
    // See https://mariadb.com/kb/en/subqueries-in-a-from-clause/
    QueryDto queryDto = Query.from(subQuery)
            .select(Collections.emptyList(), List.of(avg("avg", "score_sum")))
            .build();
    Table result = this.executor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(132d));
  }

  @Test
  void testSubQueryAggIfWithConditionOnSubQueryField() {
    QueryDto subQuery = Query.from("student")
            .select(tableFields(List.of("name")), List.of(sum("score_sum", "score")))
            .build();

    // Take into account only score.sum >= 100
    AggregatedMeasure avg = new AggregatedMeasure("avg", "score_sum", AVG, criterion("score_sum", ge(new ConstantField(100.0))));

    QueryDto queryDto = Query.from(subQuery)
            .select(Collections.emptyList(), List.of(avg))
            .build();
    Table result = this.executor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(160d));
  }

  @Test
  void testSubQueryAndRollup() {
    // This sub-query does not really make sense in that case, but the idea is to have 1 remaining column in the
    // top-select to do a rollup afterwards.
    QueryDto subQuery = Query.from("student")
            .select(tableFields(List.of("name", "score")), List.of(min("score_min", "score")))
            .build();

    QueryDto queryDto = Query.from(subQuery)
            .select(tableFields(List.of("name")), List.of(avg("avg", "score_min")))
            .rollup(tableFields(List.of("name")))
            .build();
    Table result = this.executor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, 66d),
            List.of("Paul", 74d),
            List.of("Peter", 38d),
            List.of("Tatiana", 86d));
  }
}
