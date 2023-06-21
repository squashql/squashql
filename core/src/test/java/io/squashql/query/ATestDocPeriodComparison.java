package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.Period;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Field;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;

/**
 * This test class is used to verify and print tables for the documentation. Nothing is asserted in those tests this is
 * why it is @{@link Disabled}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestClass(ignore = {TestClass.Type.SPARK, TestClass.Type.BIGQUERY, TestClass.Type.SNOWFLAKE, TestClass.Type.CLICKHOUSE})
public abstract class ATestDocPeriodComparison extends ABaseTestQuery {

  @Override
  protected Map<String, List<Field>> getFieldsByStore() {
    Field studentName = new Field("student", "name", String.class);
    Field test = new Field("student", "test", String.class);
    Field score = new Field("student", "score", int.class);
    Field semester = new Field("student", "semester", int.class);
    Field year = new Field("student", "year", int.class);
    return Map.of("student", List.of(studentName, test, score, year, semester));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, "student", List.of(
            // 2022 - s1
            new Object[]{"Paul", "mathematics", 75, 2022, 1},
            new Object[]{"Paul", "english", 73, 2022, 1},
            new Object[]{"Tatiana", "mathematics", 87, 2022, 1},
            new Object[]{"Tatiana", "english", 83, 2022, 1},

            // 2022 - s2
            new Object[]{"Paul", "mathematics", 58, 2022, 2},
            new Object[]{"Paul", "english", 70, 2022, 2},
            new Object[]{"Tatiana", "mathematics", 65, 2022, 2},
            new Object[]{"Tatiana", "english", 65, 2022, 2},

            // 2023 - s1
            new Object[]{"Paul", "mathematics", 70, 2023, 1},
            new Object[]{"Paul", "english", 82, 2023, 1},
            new Object[]{"Tatiana", "mathematics", 52, 2023, 1},
            new Object[]{"Tatiana", "english", 96, 2023, 1},

            // 2023 - s2
            new Object[]{"Paul", "mathematics", 45, 2023, 2},
            new Object[]{"Paul", "english", 89, 2023, 2},
            new Object[]{"Tatiana", "mathematics", 14, 2023, 2},
            new Object[]{"Tatiana", "english", 63, 2023, 2}
    ));
  }

  @Test
  void testSemester() {
    Measure sum = Functions.sum("score_sum", "score");
    Measure comp = new ComparisonMeasureReferencePosition(
            "compare with previous semester",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            sum,
            Map.of("semester", "s-1"),
            new Period.Semester("semester", "year"));

    QueryDto queryDto = Query.from("student")
            .select(List.of("year", "semester", "name"), List.of(sum, comp))
            .build();
    Table result = this.executor.execute(queryDto);
    result.show();
  }

  @Test
  void testYear() {
    Measure sum = Functions.sum("score_sum", "score");
    Measure comp = new ComparisonMeasureReferencePosition(
            "compare with previous year",
            ComparisonMethod.RELATIVE_DIFFERENCE,
            sum,
            Map.of("year", "y-1"),
            new Period.Year("year"));

    QueryDto queryDto = Query.from("student")
            .select(List.of("year", "name"), List.of(sum, Functions.multiply("progression in %", comp, Functions.decimal(100))))
            .build();
    Table result = this.executor.execute(queryDto);
    result.show();
  }
}
