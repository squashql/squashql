package me.paulbares.query;

import me.paulbares.SnowflakeDatastore;
import me.paulbares.SnowflakeUtil;
import me.paulbares.query.agg.AggregationFunction;
import me.paulbares.query.database.SnowflakeEngine;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.QueryDto;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static me.paulbares.query.Functions.plus;

public class TestSaasQuery {

  String credendialsPath = "/Users/paul/dev/canvas-landing-355413-eb118aab8b19.json"; // FIXME
  String projectId = "canvas-landing-355413";
  String datasetName = "business_planning";

  @Test
  @Disabled
  void test() {
    QueryDto query = new QueryDto().table("saas");

    BucketColumnSetDto bucketColumnSetDto = new BucketColumnSetDto(
            "group",
            "scenario_encrypted");
    bucketColumnSetDto.values = Map.of("group1", List.of("A", "B", "C", "D"), "group2", List.of("A", "D"));
    Period.Year year = new Period.Year("Year");

    query.withColumnSet(ColumnSetKey.BUCKET, bucketColumnSetDto);

    AggregatedMeasure amount = new AggregatedMeasure("Amount", "Amount", AggregationFunction.SUM);
    AggregatedMeasure sales = new AggregatedMeasure("sales", "Amount", AggregationFunction.SUM, "Income_Expense", Functions.eq("Revenue"));
    query.withMeasure(amount);
    query.withMeasure(sales);
    Measure ebidtaRatio = Functions.divide("EBITDA %", amount, sales);
    query.withMeasure(ebidtaRatio);

    ComparisonMeasureReferencePosition growth = new ComparisonMeasureReferencePosition(
            "Growth",
            ComparisonMethod.DIVIDE,
            sales,
            Map.of("Year", "y-1"),
            year);
    query.withMeasure(growth);
    Measure kpi = plus("KPI", ebidtaRatio, growth);
    query.withMeasure(kpi);

    ComparisonMeasureReferencePosition kpiComp = new ComparisonMeasureReferencePosition(
            "KPI comp. with prev. scenario",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            kpi,
            Map.of("scenario_encrypted", "s-1", "group", "g"),
            ColumnSetKey.BUCKET);
    query.withMeasure(kpiComp);

//    SnowflakeEngine engine = new SnowflakeEngine(new SnowflakeDatastore(SnowflakeUtil.createCredentials(this.credendialsPath), this.projectId, this.datasetName));
//
//    QueryExecutor executor = new QueryExecutor(engine);
//    execute(() -> {
//      Table execute = executor.execute(query);
//      execute.show();
//    });
//    execute(() -> executor.execute(query));
//    execute(() -> executor.execute(query));
  }

  void execute(Runnable runnable) {
    long start = System.nanoTime();
    runnable.run();
    System.out.println("Execution time: " + Duration.ofNanos(System.nanoTime() - start).toMillis() + " ms");
  }
}
