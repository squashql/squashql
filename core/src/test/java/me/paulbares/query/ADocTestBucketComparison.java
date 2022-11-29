package me.paulbares.query;

import me.paulbares.query.builder.Query;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;

import static me.paulbares.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static me.paulbares.transaction.TransactionManager.SCENARIO_FIELD_NAME;

/**
 * This test class is used to verify and print tables for the documentation. Nothing is asserted in those tests this is
 * why it is @{@link Disabled}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled
public abstract class ADocTestBucketComparison {

  protected Datastore datastore;

  protected QueryExecutor queryExecutor;

  protected TransactionManager tm;

  protected abstract QueryEngine createQueryEngine(Datastore datastore);

  protected abstract Datastore createDatastore();

  protected abstract TransactionManager createTransactionManager();

  @BeforeAll
  void setup() {
    Field salePrice = new Field("saleprice", double.class);
    Field loavesSold = new Field("loavessold", int.class);
    Field pos = new Field("pointofsale", String.class);

    this.datastore = createDatastore();
    QueryEngine queryEngine = createQueryEngine(this.datastore);
    this.queryExecutor = new QueryExecutor(queryEngine);
    this.tm = createTransactionManager();

    beforeLoad(Map.of("store", List.of(salePrice, loavesSold, pos)));
    load();
  }

  protected void load() {
    this.tm.load(MAIN_SCENARIO_NAME, "store", List.of(new Object[]{2d, 100, "A"}, new Object[]{2d, 80, "B"}));
    this.tm.load("s1", "store", List.of(new Object[]{3d, 74, "A"}, new Object[]{3d, 50, "B"}));
    this.tm.load("s2", "store", List.of(new Object[]{4d, 55, "A"}, new Object[]{4d, 20, "B"}));
    this.tm.load("s3", "store", List.of(new Object[]{2d, 100, "A"}, new Object[]{3d, 50, "B"}));
  }

  protected void beforeLoad(Map<String, List<Field>> fieldsByStore) {
  }

  @Test
  void test() {
    Measure revenue = new ExpressionMeasure("revenue", "sum(saleprice * loavessold)");
    BucketColumnSetDto bucketCS = new BucketColumnSetDto("group", SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "s1"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "s2"))
            .withNewBucket("group3", List.of(MAIN_SCENARIO_NAME, "s3"))
            .withNewBucket("group4", List.of(MAIN_SCENARIO_NAME, "s1", "s2", "s3"));
    ComparisonMeasureReferencePosition revenueComparison = new ComparisonMeasureReferencePosition(
            "revenueComparison",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            revenue,
            Map.of(SCENARIO_FIELD_NAME, "s-1", "group", "g"),
            ColumnSetKey.BUCKET);

    QueryDto queryDto = Query.from("store")
//            .select(List.of(SCENARIO_FIELD_NAME, "saleprice", "loavessold", "pointofsale"),List.of(CountMeasure.INSTANCE, revenue))
            .select(List.of(), List.of(bucketCS), List.of(CountMeasure.INSTANCE, revenue, revenueComparison))
            .build();
    Table result = this.queryExecutor.execute(queryDto);
    result.show();
  }
}
