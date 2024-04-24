package io.squashql.query;

import io.squashql.TestClass;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.*;
import io.squashql.table.Table;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.squashql.query.Functions.*;
import static io.squashql.query.agg.AggregationFunction.AVG;
import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;

@TestClass
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestSubQuery extends ABaseTestQuery {

  final String store = "student" + getClass().getSimpleName().toLowerCase();
  final TableField studentName = new TableField(this.store, "name");
  final TableField group = new TableField(this.store, "group");
  final TableField test = new TableField(this.store, "test");
  final TableField score = new TableField(this.store, "score");

  @Override
  protected Map<String, List<TableTypedField>> getFieldsByStore() {
    // See https://mariadb.com/kb/en/subqueries-in-a-from-clause/
    TableTypedField studentName = new TableTypedField(this.store, "name", String.class);
    TableTypedField group = new TableTypedField(this.store, "group", String.class);
    TableTypedField test = new TableTypedField(this.store, "test", String.class);
    TableTypedField score = new TableTypedField(this.store, "score", int.class);
    return Map.of(this.store, List.of(group, studentName, test, score));
  }

  @Override
  protected void loadData() {
    this.tm.load(this.store, List.of(
            new Object[]{"group a", "Paul", "sql", 50},
            new Object[]{"group a", "Paul", "java", 70},
            new Object[]{"group a", "Peter", "sql", 45},
            new Object[]{"group a", "Peter", "java", 35},
            new Object[]{"group a", "Tatiana", "sql", 85},
            new Object[]{"group a", "Tatiana", "java", 75},

            new Object[]{"group b", "John", "sql", 50},
            new Object[]{"group b", "John", "java", 80},
            new Object[]{"group b", "William", "sql", 60},
            new Object[]{"group b", "William", "java", 90},
            new Object[]{"group b", "Kate", "sql", 70},
            new Object[]{"group b", "Kate", "java", 70},

            new Object[]{"group c", "Paul", "sql", 35},
            new Object[]{"group c", "Paul", "java", 25},
            new Object[]{"group c", "Liam", "sql", 56},
            new Object[]{"group c", "Liam", "java", 24},
            new Object[]{"group c", "Scott", "sql", 80},
            new Object[]{"group c", "Scott", "java", 80}
    ));
  }

  @Test
  void testSubQuery() {
    QueryDto subQuery = Query.from(this.store)
            .where(criterion(this.group, eq("group a")))
            .select(List.of(this.studentName), List.of(sum("score_sum", this.score)))
            .build();
    // See https://mariadb.com/kb/en/subqueries-in-a-from-clause/
    QueryDto queryDto = Query.from(subQuery)
            .select(Collections.emptyList(), List.of(avg("avg", "score_sum")))
            .build();
    Table result = this.executor.executeQuery(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(120d));
  }

  @Test
  void testSubQueryWithAlias() {
    Field tableField = this.studentName.as("student_name");
    QueryDto subQuery = Query.from(this.store)
            .where(criterion(this.group, eq("group a")))
            .select(List.of(tableField), List.of(sum("score_sum", this.score)))
            .build();
    // See https://mariadb.com/kb/en/subqueries-in-a-from-clause/
    QueryDto queryDto = Query.from(subQuery)
            .select(List.of(new AliasedField("student_name")), List.of(avg("avg", "score_sum")))
            .build();
    Table result = this.executor.executeQuery(queryDto);
    Assertions.assertThat(result.headers().stream().map(Header::name)).containsExactly("student_name", "avg");
    Assertions.assertThat(result).containsExactly(
            List.of("Paul", 120d),
            List.of("Peter", 80d),
            List.of("Tatiana", 160d));
  }

  @Test
  void testSubQueryAggIfWithConditionOnSubQueryField() {
    QueryDto subQuery = Query.from(this.store)
            .where(criterion(this.group, eq("group a")))
            .select(List.of(this.studentName), List.of(sum("score_sum", this.score)))
            .build();

    // Take into account only score.sum >= 100
    AggregatedMeasure avg = new AggregatedMeasure("avg", "score_sum", AVG, criterion("score_sum", ge(100.0)));

    QueryDto queryDto = Query.from(subQuery)
            .select(Collections.emptyList(), List.of(avg))
            .build();
    Table result = this.executor.executeQuery(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(140d));
  }

  @Test
  void testSubQueryAndRollup() {
    // This sub-query does not really make sense in that case, but the idea is to have 1 remaining column in the
    // top-select to do a rollup afterward.
    QueryDto subQuery = Query.from(this.store)
            .where(criterion(this.group, eq("group a")))
            .select(List.of(this.studentName, this.score), List.of(min("score_min", this.score)))
            .build();

    QueryDto queryDto = Query.from(subQuery)
            .select(List.of(new AliasedField(this.studentName.fieldName)), List.of(avg("avg", "score_min")))
            .rollup(List.of(new AliasedField(this.studentName.fieldName)))
            .build();
    Table result = this.executor.executeQuery(queryDto);
    Assertions.assertThat(result).containsExactly(
            List.of(GRAND_TOTAL, 60d),
            List.of("Paul", 60d),
            List.of("Peter", 40d),
            List.of("Tatiana", 80d));
  }

  @Test
  void testTwoNestedSubQueries() {
    QueryDto subQuery2 = Query.from(this.store)
            .select(List.of(this.group, this.studentName), List.of(avg("score_avg_student", this.score)))
            .build();

    QueryDto subQuery1 = Query.from(subQuery2)
            .select(List.of(new AliasedField(this.group.fieldName)), List.of(avg("score_avg_group", new AliasedField("score_avg_student"))))
            .build();

    Table result = this.executor.executeQuery(subQuery1);
    Assertions.assertThat(result).containsExactly(
            List.of("group a", 60d),
            List.of("group b", 70d),
            List.of("group c", 50d));

    QueryDto query = Query.from(subQuery1)
            .select(List.of(), List.of(avg("score_avg", new AliasedField("score_avg_group"))))
            .build();
    result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of(60d));
  }

  @Test
  void testTwoNestedSubQueriesAndBucketing() {
    // CLICKHOUSE does not support non-equi join
    Assumptions.assumeFalse(this.queryEngine.getClass().getSimpleName().contains(TestClass.Type.CLICKHOUSE.className));

    QueryDto subQuery2 = Query.from(this.store)
            .select(List.of(this.group, this.studentName), List.of(avg("score_avg_student", this.score)))
            .build();

    QueryDto subQuery1 = Query.from(subQuery2)
            .select(List.of(new AliasedField(this.group.fieldName)), List.of(avg("score_avg_group", new AliasedField("score_avg_student"))))
            .build();

    VirtualTableDto levels = new VirtualTableDto(
            "levels",
            List.of("level", "min", "max"),
            List.of(
                    List.of("good", 60d, 101d),
                    List.of("bad", 0d, 60d)
            ));

    CriteriaDto criteria = all(
            criterion(new AliasedField("score_avg_group"), new TableField("levels.min"), ConditionType.GE),
            criterion(new AliasedField("score_avg_group"), new TableField("levels.max"), ConditionType.LT));

    QueryDto query = Query.from(subQuery1)
            .join(levels, JoinType.INNER)
            .on(criteria)
            .select(List.of(new TableField("levels.level")), List.of(CountMeasure.INSTANCE))
            .build();
    Table result = this.executor.executeQuery(query);
    Assertions.assertThat(result).containsExactly(
            List.of("bad", 1L),
            List.of("good", 2L));
  }
}
