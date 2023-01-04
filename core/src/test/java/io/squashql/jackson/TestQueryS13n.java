package io.squashql.jackson;

import io.squashql.query.*;
import io.squashql.query.context.ContextValue;
import io.squashql.query.context.QueryCacheContextValue;
import io.squashql.query.dto.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.squashql.query.ComparisonMethod.ABSOLUTE_DIFFERENCE;
import static io.squashql.query.Functions.*;
import static io.squashql.transaction.TransactionManager.MAIN_SCENARIO_NAME;
import static io.squashql.transaction.TransactionManager.SCENARIO_FIELD_NAME;

public class TestQueryS13n {

  @Test
  void testRoundTrip() {
    QueryDto query = new QueryDto()
            .table("myTable")
            .withColumn(SCENARIO_FIELD_NAME)
            .withColumn("ean")
            .aggregatedMeasure("p", "price", "sum")
            .aggregatedMeasure("q", "quantity", "sum")
            .aggregatedMeasure("priceAlias", "price", "sum", Functions.criterion("category", Functions.eq("food")))
            .expressionMeasure("alias1", "firstMyExpression")
            .expressionMeasure("alias2", "secondMyExpression")
            .withMeasure(new BinaryOperationMeasure("plus1",
                    BinaryOperator.PLUS,
                    new AggregatedMeasure("p", "price", "sum"),
                    new AggregatedMeasure("p", "price", "sum")))
            .context(QueryCacheContextValue.KEY, new QueryCacheContextValue(QueryCacheContextValue.Action.NOT_USE));

    String serialize = query.json();
    QueryDto deserialize = JacksonUtil.deserialize(serialize, QueryDto.class);
    Assertions.assertThat(deserialize).isEqualTo(query);
  }

  @Test
  void testRoundTripWithJoinsAndConditions() {
    QueryDto query = new QueryDto();

    // Table
    var orders = new TableDto("orders");
    var orderDetails = new TableDto("orderDetails");

    // Join
    orders.innerJoin(orderDetails, "orderDetailsId", "orderDetailsId");

    query.table(orders);

    // Coordinates
    query.withColumn("productName");
    query.withColumn("categoryName");

    // Measures
    query.aggregatedMeasure("p", "price", "sum");
    query.expressionMeasure("alias", "expression");

    // Conditions
    ConditionDto december = and(gt("1/12/1996"), lt("31/12/1996"));
    ConditionDto october = and(ge("1/10/1996"), le("31/10/1996"));
    query.withCondition("orderDate", or(december, october));
    query.withCondition("city", in("paris", "london"));
    query.withCondition("country", eq("france"));
    query.withCondition("shipper", neq("aramex"));

    String serialize = query.json();
    QueryDto deserialize = JacksonUtil.deserialize(serialize, QueryDto.class);
    Assertions.assertThat(deserialize).isEqualTo(query);
  }

  @Test
  void testRoundTripBucketComparisonQuery() {
    String groupOfScenario = "Group of scenario";
    BucketColumnSetDto bucketCS = new BucketColumnSetDto(groupOfScenario, SCENARIO_FIELD_NAME)
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "s1"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "s2"))
            .withNewBucket("group3", List.of(MAIN_SCENARIO_NAME, "s1", "s2"));

    AggregatedMeasure price = new AggregatedMeasure("p", "price", "sum");
    ComparisonMeasureReferencePosition priceComp = new ComparisonMeasureReferencePosition(
            "priceDiff",
            ABSOLUTE_DIFFERENCE,
            price,
            Map.of(
                    SCENARIO_FIELD_NAME, "first",
                    groupOfScenario, "g"
            ),
            ColumnSetKey.BUCKET);

    var query = new QueryDto()
            .table("products")
            .withColumnSet(ColumnSetKey.BUCKET, bucketCS)
            .withMeasure(priceComp)
            .withMeasure(price);

    String serialize = query.json();
    QueryDto deserialize = JacksonUtil.deserialize(serialize, QueryDto.class);
    Assertions.assertThat(deserialize).isEqualTo(query);
  }

  @Test
  void testRoundTripPeriodComparisonQuery() {
    AggregatedMeasure sales = new AggregatedMeasure("s", "sales", "sum");
    Period.Quarter period = new Period.Quarter("quarter_sales", "year_sales");
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of("year_sales", "y-1"),
            period);

    var query = new QueryDto()
            .table("products")
            .withColumn(SCENARIO_FIELD_NAME)
            .withMeasure(m)
            .withMeasure(sales);

    String serialize = query.json();
    QueryDto deserialize = JacksonUtil.deserialize(serialize, QueryDto.class);
    Assertions.assertThat(deserialize).isEqualTo(query);
  }

  @Test
  void testQueryWithComparator() {
    var query = new QueryDto()
            .table("products")
            .withMeasure(new AggregatedMeasure("s", "sales", "sum"));

    query.orderBy("X", List.of("a", "b", "c"));
    query.orderBy("Y", OrderKeywordDto.ASC);

    String serialize = query.json();
    QueryDto deserialize = JacksonUtil.deserialize(serialize, QueryDto.class);
    Assertions.assertThat(deserialize).isEqualTo(query);
  }

  @Test
  void testConditions() {
    ConditionDto c1 = new SingleValueConditionDto(ConditionType.EQ, 5);
    String serialize = JacksonUtil.serialize(c1);
    ConditionDto deserialize = JacksonUtil.deserialize(serialize, ConditionDto.class);
    Assertions.assertThat(deserialize).isEqualTo(c1);

    ConditionDto december = and(gt("1/12/1996"), lt("31/12/1996"));
    ConditionDto october = and(ge("1/10/1996"), le("31/10/1996"));
    ConditionDto c2 = or(december, october);
    serialize = JacksonUtil.serialize(c2);
    deserialize = JacksonUtil.deserialize(serialize, ConditionDto.class);
    Assertions.assertThat(deserialize).isEqualTo(c2);

    ConditionDto c3 = in("paris", "london");
    deserialize = JacksonUtil.deserialize(JacksonUtil.serialize(c3), ConditionDto.class);
    Assertions.assertThat(deserialize).isEqualTo(c3);
  }

  @Test
  void testOrders() {
    OrderDto o = new ExplicitOrderDto(List.of("a", "b"));
    OrderDto deserialize = JacksonUtil.deserialize(JacksonUtil.serialize(o), OrderDto.class);
    Assertions.assertThat(deserialize).isEqualTo(o);

    o = new SimpleOrderDto(OrderKeywordDto.DESC);
    deserialize = JacksonUtil.deserialize(JacksonUtil.serialize(o), OrderDto.class);
    Assertions.assertThat(deserialize).isEqualTo(o);
  }

  @Test
  void testContextValues() {
    ContextValue cv = new QueryCacheContextValue(QueryCacheContextValue.Action.USE);
    ContextValue deserialize = JacksonUtil.deserialize(JacksonUtil.serialize(cv), ContextValue.class);
    Assertions.assertThat(deserialize).isEqualTo(cv);
  }
}
