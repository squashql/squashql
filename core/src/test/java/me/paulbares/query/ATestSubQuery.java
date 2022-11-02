package me.paulbares.query;

import me.paulbares.query.builder.Query;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
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
    Field studentName = new Field("name", String.class);
    Field test = new Field("test", String.class);
    Field score = new Field("score", int.class);

    this.datastore = createDatastore();
    QueryEngine queryEngine = createQueryEngine(this.datastore);
    this.queryExecutor = new QueryExecutor(queryEngine);
    this.tm = createTransactionManager();

    beforeLoad(Map.of("student", List.of(studentName, test, score)));
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
  }

  protected void beforeLoad(Map<String, List<Field>> fieldsByStore) {
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
    Table result = this.queryExecutor.execute(queryDto);
    Assertions.assertThat(result).containsExactly(List.of(130.66666666666666d));
  }

  @Test
  void testSubQueryAggIfWithConditionOnSubQueryField() {
    QueryDto subQuery = Query.from("student")
            .select(List.of("name"), List.of(sum("score_sum", "score")))
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
