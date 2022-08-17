package me.paulbares.query;

import me.paulbares.BigQueryDatastore;
import me.paulbares.BigQueryUtil;
import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.database.BigQueryEngine;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.QueryDto;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static me.paulbares.query.QueryBuilder.*;

public class TestSaasQuery {

  String credendialsPath = "/Users/paul/dev/canvas-landing-355413-eb118aab8b19.json"; // FIXME
  String projectId = "canvas-landing-355413";
  String datasetName = "business_planning";

  @Test
  @Disabled
  void test() {
    QueryDto query = QueryBuilder.query().table("saas");

    QueryBuilder.addBucketColumnSet(query,
            "group",
            "scenario_encrypted",
            Map.of("group1", List.of("A", "B", "C", "D"), "group2", List.of("A", "D")));
    QueryBuilder.addPeriodColumnSet(query, new Period.Year("Year"));

    AggregatedMeasure amount = new AggregatedMeasure("Amount", AggregationFunction.SUM);
    AggregatedMeasure sales = new AggregatedMeasure("sales", "Amount", AggregationFunction.SUM, "Income_Expense", QueryBuilder.eq("Revenue"));
    query.withMeasure(amount);
    query.withMeasure(sales);
    Measure ebidtaRatio = QueryBuilder.divide("EBITDA %", amount, sales);
    query.withMeasure(ebidtaRatio);

    ComparisonMeasure growth = periodComparison(
            "Growth",
            ComparisonMethod.DIVIDE,
            sales,
            Map.of("Year", "y-1"));
    query.withMeasure(growth);
    Measure kpi = plus("KPI", ebidtaRatio, growth);
    query.withMeasure(kpi);

    ComparisonMeasure kpiComp = bucketComparison(
            "KPI comp. with prev. scenario",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            kpi,
            Map.of("scenario_encrypted", "s-1", "group", "g"));
    query.withMeasure(kpiComp);

    BigQueryEngine engine = new BigQueryEngine(new BigQueryDatastore(BigQueryUtil.createCredentials(this.credendialsPath), this.projectId, this.datasetName));

    QueryExecutor executor = new QueryExecutor(engine);
    execute(() -> executor.execute(query));
    execute(() -> executor.execute(query));
    execute(() -> executor.execute(query));
  }

  void execute(Runnable runnable) {
    long start = System.nanoTime();
    runnable.run();
    System.out.println("Execution time: " + Duration.ofNanos(System.nanoTime() - start).toMillis() + " ms");
  }
}
