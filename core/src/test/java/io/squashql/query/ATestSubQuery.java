package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.squashql.query.Functions.*;
import static io.squashql.query.agg.AggregationFunction.AVG;
import static io.squashql.transaction.TransactionManager.MAIN_SCENARIO_NAME;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestSubQuery extends ABaseTestQuery {

  @Override
  protected Map<String, List<Field>> getFieldsByStore() {
    // See https://mariadb.com/kb/en/subqueries-in-a-from-clause/
    Field studentName = new Field("name", String.class);
    Field test = new Field("test", String.class);
    Field score = new Field("score", int.class);
    return Map.of("student", List.of(studentName, test, score));
  }

  @Override
  protected void loadData() {
    this.tm.load(MAIN_SCENARIO_NAME, "student", List.of(
            new Object[] {"Paul", "sql", 75},
            new Object[] {"Paul", "java", 73},
            new Object[] {"Peter", "sql", 43},
            new Object[] {"Peter", "java", 31},
            new Object[] {"Tatiana", "sql", 87},
            new Object[] {"Tatiana", "java", 83}
    ));
  }

  @Test
  void testSubQuery() {
    QueryDto subQuery = Query.from("student")
            .select(List.of("name"), List.of(sum("score_sum", "score")))
            .build();
    // See https://mariadb.com/kb/en/subqueries-in-a-from-clause/
    QueryDto queryDto = Query.from(subQuery)
            .select(Collections.emptyList(), List.of(avg("avg", "score_sum")))
            .build();
    Table result = this.executor.execute(queryDto);
    Assertions.assertThat(result).usingElementComparator(
                    (left, right) -> {
                      if (left.size() == right.size()) {
                        for (int i = 0; i < left.size(); i++) {
                          if (Math.abs((Double) left.get(i) - (Double) right.get(i)) > 1E-6) {
                            return (Double) left.get(i) < (Double) right.get(i) ? -1 : 1;
                          }
                        }
                        return 0;
                      }
                      return left.size() < right.size() ? -1 : 1;
                    })
            .containsExactly(List.of(130.66666666666666d));
  }

  @Test
  void testSubQueryAggIfWithConditionOnSubQueryField() {
    QueryDto subQuery = Query.from("student")
            .select(List.of("name"), List.of(sum("score_sum", "score")))
            .build();

    // Take into account only score.sum >= 100
    AggregatedMeasure avg = new AggregatedMeasure("avg", "score_sum", AVG, criterion("score_sum", ge(100.0)));

    QueryDto queryDto = Query.from(subQuery)
            .select(Collections.emptyList(), List.of(avg))
            .build();
    Table result = this.executor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(159d));
  }
}
