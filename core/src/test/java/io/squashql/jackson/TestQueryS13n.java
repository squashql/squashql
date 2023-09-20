package io.squashql.jackson;

import static io.squashql.query.ComparisonMethod.ABSOLUTE_DIFFERENCE;
import static io.squashql.query.Functions.and;
import static io.squashql.query.Functions.eq;
import static io.squashql.query.Functions.ge;
import static io.squashql.query.Functions.gt;
import static io.squashql.query.Functions.in;
import static io.squashql.query.Functions.le;
import static io.squashql.query.Functions.lt;
import static io.squashql.query.Functions.neq;
import static io.squashql.query.Functions.or;
import static io.squashql.query.TableField.tableField;
import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;
import static io.squashql.transaction.DataLoader.SCENARIO_FIELD_NAME;

import io.squashql.query.AggregatedMeasure;
import io.squashql.query.BinaryOperationMeasure;
import io.squashql.query.BinaryOperator;
import io.squashql.query.ColumnSetKey;
import io.squashql.query.ComparisonMeasureReferencePosition;
import io.squashql.query.ExpressionMeasure;
import io.squashql.query.Functions;
import io.squashql.query.TableField;
import io.squashql.query.dto.BucketColumnSetDto;
import io.squashql.query.dto.ConditionDto;
import io.squashql.query.dto.ConditionType;
import io.squashql.query.dto.ExplicitOrderDto;
import io.squashql.query.dto.JoinMappingDto;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.OrderDto;
import io.squashql.query.dto.OrderKeywordDto;
import io.squashql.query.dto.Period;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.SimpleOrderDto;
import io.squashql.query.dto.SingleValueConditionDto;
import io.squashql.query.dto.TableDto;
import io.squashql.query.parameter.Parameter;
import io.squashql.query.parameter.QueryCacheParameter;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestQueryS13n {

  @Test
  void testRoundTrip() {
    QueryDto query = new QueryDto()
            .table("myTable")
            .withColumn(tableField(SCENARIO_FIELD_NAME))
            .withColumn(tableField("ean"))
            .withMeasure(new AggregatedMeasure("p", "price", "sum"))
            .withMeasure(new AggregatedMeasure("q", "quantity", "sum"))
            .withMeasure(new AggregatedMeasure("priceAlias", "price", "sum", Functions.criterion("category", Functions.eq("food"))))
            .withMeasure(new ExpressionMeasure("alias1", "firstMyExpression"))
            .withMeasure(new ExpressionMeasure("alias2", "secondMyExpression"))
            .withMeasure(new BinaryOperationMeasure("plus1",
                    BinaryOperator.PLUS,
                    new AggregatedMeasure("p", "price", "sum"),
                    new AggregatedMeasure("p", "price", "sum")))
            .withParameter(QueryCacheParameter.KEY, new QueryCacheParameter(QueryCacheParameter.Action.NOT_USE));

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
    orders.join(orderDetails, JoinType.INNER, new JoinMappingDto("orderDetailsId", "orderDetailsId", ConditionType.EQ));

    query.table(orders);

    // Coordinates
    query.withColumn(tableField("productName"));
    query.withColumn(tableField("categoryName"));

    // Measures
    query.withMeasure(new AggregatedMeasure("p", "price", "sum"));
    query.withMeasure(new ExpressionMeasure("alias", "expression"));

    // Conditions
    ConditionDto december = and(gt("1/12/1996"), lt("31/12/1996"));
    ConditionDto october = and(ge("1/10/1996"), le("31/10/1996"));
    query.withCondition(tableField("orderDate"), or(december, october));
    query.withCondition(tableField("city"), in("paris", "london"));
    query.withCondition(tableField("country"), eq("france"));
    query.withCondition(tableField("shipper"), neq("aramex"));

    String serialize = query.json();
    QueryDto deserialize = JacksonUtil.deserialize(serialize, QueryDto.class);
    Assertions.assertThat(deserialize).isEqualTo(query);
  }

  @Test
  void testRoundTripBucketComparisonQuery() {
    String groupOfScenario = "Group of scenario";
    BucketColumnSetDto bucketCS = new BucketColumnSetDto(groupOfScenario, tableField(SCENARIO_FIELD_NAME))
            .withNewBucket("group1", List.of(MAIN_SCENARIO_NAME, "s1"))
            .withNewBucket("group2", List.of(MAIN_SCENARIO_NAME, "s2"))
            .withNewBucket("group3", List.of(MAIN_SCENARIO_NAME, "s1", "s2"));

    AggregatedMeasure price = new AggregatedMeasure("p", "price", "sum");
    ComparisonMeasureReferencePosition priceComp = new ComparisonMeasureReferencePosition(
            "priceDiff",
            ABSOLUTE_DIFFERENCE,
            price,
            Map.of(
                    tableField(SCENARIO_FIELD_NAME), "first",
                    tableField(groupOfScenario), "g"
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
    Period.Quarter period = new Period.Quarter(tableField("quarter_sales"), tableField("year_sales"));
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition(
            "myMeasure",
            ABSOLUTE_DIFFERENCE,
            sales,
            Map.of(period.year(), "y-1"),
            period);

    var query = new QueryDto()
            .table("products")
            .withColumn(tableField(SCENARIO_FIELD_NAME))
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

    query.orderBy(tableField("X"), List.of("a", "b", "c"));
    query.orderBy(tableField("Y"), OrderKeywordDto.ASC);

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
  void testParameter() {
    Parameter cv = new QueryCacheParameter(QueryCacheParameter.Action.USE);
    Parameter deserialize = JacksonUtil.deserialize(JacksonUtil.serialize(cv), Parameter.class);
    Assertions.assertThat(deserialize).isEqualTo(cv);
  }

  @Test
  void testTableField() {
    TableField fieldFullName = new TableField("table.name");
    TableField simpleNameField = new TableField("name");
    TableField field = new TableField("table","name");
    TableField fieldFullNameDeserialize = JacksonUtil.deserialize(JacksonUtil.serialize(fieldFullName), TableField.class);
    Assertions.assertThat(fieldFullNameDeserialize).isEqualTo(fieldFullName);
    TableField fieldDeserialize = JacksonUtil.deserialize(JacksonUtil.serialize(field), TableField.class);
    Assertions.assertThat(fieldDeserialize).isEqualTo(field);
    TableField simpleFieldDeserialize = JacksonUtil.deserialize(JacksonUtil.serialize(simpleNameField), TableField.class);
    Assertions.assertThat(simpleFieldDeserialize).isEqualTo(simpleNameField);
  }
}
