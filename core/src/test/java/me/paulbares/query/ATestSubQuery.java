package me.paulbares.query;

import me.paulbares.query.builder.Query;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.TypedField;
import me.paulbares.transaction.TransactionManager;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static me.paulbares.query.Functions.*;
import static me.paulbares.query.agg.AggregationFunction.AVG;
import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ATestSubQuery {

  protected Datastore datastore;

  protected QueryExecutor queryExecutor;

  protected TransactionManager tm;

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore();

  protected abstract TransactionManager createTransactionManager();

  @BeforeAll
  void setup() {
    // See https://mariadb.com/kb/en/subqueries-in-a-from-clause/
    TypedField studentName = new TypedField("studentName", String.class);
    TypedField test = new TypedField("test", String.class);
    TypedField score = new TypedField("score", int.class);

    // See https://www.geeksforgeeks.org/sql-sub-queries-clause/
    TypedField professorName = new TypedField("professorName", String.class);
    TypedField department = new TypedField("department", String.class);
    TypedField salary = new TypedField("salary", int.class);
    TypedField budget = new TypedField("budget", int.class);
    TypedField departmentName = new TypedField("departmentName", String.class);

    this.datastore = createDatastore();
    QueryEngine queryEngine = createQueryEngine(this.datastore);
    this.queryExecutor = new QueryExecutor(queryEngine);
    this.tm = createTransactionManager();

    beforeLoad(
            Map.of("student", List.of(studentName, test, score),
                    "professor", List.of(professorName, department, salary),
                    "department", List.of(departmentName, budget)));
    load();
  }

  protected void load() {
    this.tm.load(MAIN_SCENARIO_NAME, "student", List.of(
            new Object[]{"Paul", "sql", 75},
            new Object[]{"Paul", "java", 73},
            new Object[]{"Peter", "sql", 43},
            new Object[]{"Peter", "java", 31},
            new Object[]{"Tatiana", "sql", 87},
            new Object[]{"Tatiana", "java", 83}
    ));

    this.tm.load(MAIN_SCENARIO_NAME, "professor", List.of(
            new Object[]{"Smith", "Computer Science", 95_000},
            new Object[]{"Bill", "Electrical", 55_000},
            new Object[]{"Sam", "Humanities", 44_000},
            new Object[]{"Erik", "Mechanical", 80_000},
            new Object[]{"Melisa", "Information Technology", 65_000},
            new Object[]{"Jena", "Civil", 50_000}
    ));

    this.tm.load(MAIN_SCENARIO_NAME, "department", List.of(
            new Object[]{"Computer Science", 100_000},
            new Object[]{"Electrical", 80_000},
            new Object[]{"Humanities", 50_000},
            new Object[]{"Mechanical", 40_000},
            new Object[]{"Information Technology", 90_000},
            new Object[]{"Civil", 60_000}
    ));
  }

  protected void beforeLoad(Map<String, List<TypedField>> fieldsByStore) {
  }

  @Test
  void testSubQuery() {
    QueryDto subQuery = Query.from("student")
            .select(List.of("studentName"), List.of(sum("score.sum", "score")))
            .build();
    // See https://mariadb.com/kb/en/subqueries-in-a-from-clause/
    QueryDto queryDto = Query.from(subQuery)
            .select(Collections.emptyList(), List.of(avg("avg", "score.sum")))
            .build();
    Table result = this.queryExecutor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(130.66666666666666d));
  }

  @Test
  void testSubQueryAggIfWithConditionOnSubQueryField() {
    QueryDto subQuery = Query.from("student")
            .select(List.of("studentName"), List.of(sum("score_sum", "score")))
            .build();

    // Take into account only score.sum >= 100
    AggregatedMeasure avg = new AggregatedMeasure("avg", "score_sum", AVG, "score_sum", Functions.ge(100.0));

    QueryDto queryDto = Query.from(subQuery)
            .select(Collections.emptyList(), List.of(avg))
            .build();
    Table result = this.queryExecutor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(159d));
  }
}
