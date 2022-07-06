package me.paulbares.query;

import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.QueryDto;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestQueryCache {

  @Test
  void test() {
    QueryEngine engine = Mockito.mock(QueryEngine.class);
    QueryExecutor executor = new QueryExecutor(engine);

    QueryDto queryDto = QueryBuilder.query()
            .table("myTable")
            .withColumn("a")
            .withMeasure(new AggregatedMeasure("b", AggregationFunction.SUM));
    Table execute = executor.execute(queryDto);
    System.out.println();
  }
}
