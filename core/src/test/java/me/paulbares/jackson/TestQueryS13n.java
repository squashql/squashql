package me.paulbares.jackson;

import me.paulbares.query.AggregatedMeasure;
import me.paulbares.query.BinaryOperationMeasure;
import me.paulbares.query.QueryBuilder;
import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.context.Totals;
import me.paulbares.query.dto.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static me.paulbares.query.QueryBuilder.*;
import static me.paulbares.store.Datastore.MAIN_SCENARIO_NAME;
import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

public class TestQueryS13n {

  @Test
  void testRoundTrip() {
    QueryDto query = new QueryDto()
            .table("myTable")
            .coordinate(SCENARIO_FIELD_NAME, "s1")
            .coordinates("city", "paris", "london")
            .wildcardCoordinate("ean")
            .aggregatedMeasure("price", "sum")
            .aggregatedMeasure("quantity", "sum")
            .expressionMeasure("alias1", "firstMyExpression")
            .expressionMeasure("alias2", "secondMyExpression")
            .context(Totals.KEY, QueryBuilder.BOTTOM);

    String serialize = query.json();
    QueryDto deserialize = JacksonUtil.deserialize(serialize, QueryDto.class);
    Assertions.assertThat(deserialize).isEqualTo(query);
  }

  @Test
  void testRoundTripWithJoinsAndConditions() {
    QueryDto query = query();

    // Table
    var orders = table("orders");
    var orderDetails = table("orderDetails");

    // Join
    orders.innerJoin(orderDetails, "orderDetailsId", "orderDetailsId");

    query.table(orders);

    // Coordinates
    query.wildcardCoordinate("productName");
    query.coordinates("categoryName", "first", "second");

    // Measures
    query.aggregatedMeasure("price", "sum");
    query.expressionMeasure("alias", "expression");

    // Conditions
    ConditionDto december = and(gt("1/12/1996"), lt("31/12/1996"));
    ConditionDto october = and(ge("1/10/1996"), le("31/10/1996"));
    query.condition("orderDate", or(december, october));
    query.condition("city", in("paris", "london"));
    query.condition("country", eq("france"));
    query.condition("shipper", neq("aramex"));

    String serialize = query.json();
    QueryDto deserialize = JacksonUtil.deserialize(serialize, QueryDto.class);
    Assertions.assertThat(deserialize.table).isEqualTo(query.table);
    Assertions.assertThat(deserialize.conditions).isEqualTo(query.conditions);
    Assertions.assertThat(deserialize.context).isEqualTo(query.context);
    Assertions.assertThat(deserialize.coordinates).isEqualTo(query.coordinates);
    Assertions.assertThat(deserialize.measures).isEqualTo(query.measures);
  }

  @Test
  void testRoundTripBucketComparisonQuery() {
    String groupOfScenario = "Group of scenario";
    BucketColumnSetDto bucketCS = new BucketColumnSetDto(groupOfScenario, SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "s1"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "s2"))
            .withNewBucket("group3", List.of(MAIN_SCENARIO_NAME, "s1", "s2"));

    AggregatedMeasure price = new AggregatedMeasure("price", "sum");
    BinaryOperationMeasure priceComp = new BinaryOperationMeasure(
            "priceDiff",
            BinaryOperations.ABS_DIFF,
            price,
            Map.of(
                    SCENARIO_FIELD_NAME, "first",
                    groupOfScenario, "g"
            ));

    var query = new NewQueryDto()
            .table("products")
            .withColumnSet(NewQueryDto.BUCKET, bucketCS)
            .withMetric(priceComp)
            .withMetric(price);

    String serialize = query.json();
    NewQueryDto deserialize = JacksonUtil.deserialize(serialize, NewQueryDto.class);
    Assertions.assertThat(deserialize).isEqualTo(query);
  }

  @Test
  void testRoundTripPeriodBucketingComparisonQuery() {
    AggregatedMeasure sales = new AggregatedMeasure("sales", "sum");
    BinaryOperationMeasure m = new BinaryOperationMeasure(
            "myMeasure",
            BinaryOperations.ABS_DIFF,
            sales,
            Map.of("year_sales", "y-1"));

    Period.Quarter period = new Period.Quarter("quarter_sales", "year_sales");
    PeriodColumnSetDto periodCS = new PeriodColumnSetDto(period);

    var query = new NewQueryDto()
            .table("products")
            .withColumn(SCENARIO_FIELD_NAME)
            .withColumnSet(NewQueryDto.PERIOD, periodCS)
            .withMetric(m)
            .withMetric(sales);

    String serialize = query.json();
    NewQueryDto deserialize = JacksonUtil.deserialize(serialize, NewQueryDto.class);
    Assertions.assertThat(deserialize).isEqualTo(query);
  }
}
