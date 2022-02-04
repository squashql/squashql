package me.paulbares.jackson;

import me.paulbares.query.AggregatedMeasure;
import me.paulbares.query.QueryBuilder;
import me.paulbares.query.context.Totals;
import me.paulbares.query.dto.ConditionDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.ScenarioComparisonDto;
import me.paulbares.query.dto.ScenarioGroupingQueryDto;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static me.paulbares.query.QueryBuilder.and;
import static me.paulbares.query.QueryBuilder.comparison;
import static me.paulbares.query.QueryBuilder.eq;
import static me.paulbares.query.QueryBuilder.ge;
import static me.paulbares.query.QueryBuilder.gt;
import static me.paulbares.query.QueryBuilder.in;
import static me.paulbares.query.QueryBuilder.le;
import static me.paulbares.query.QueryBuilder.lt;
import static me.paulbares.query.QueryBuilder.neq;
import static me.paulbares.query.QueryBuilder.or;
import static me.paulbares.query.QueryBuilder.query;
import static me.paulbares.query.QueryBuilder.scenarioComparisonQuery;
import static me.paulbares.query.QueryBuilder.table;
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
  void testRoundTripScenarioComparisonQuery() {
    var query = scenarioComparisonQuery()
            .table("products")
            .defineNewGroup("group1", "base", "s1")
            .defineNewGroup("group2", "base", "s1", "s2")
            .defineNewGroup("group3", "base", "s2");

    ScenarioComparisonDto comparison = comparison(
            "absolute_difference",
            new AggregatedMeasure("price", "sum"),
            true,
            "first");

    query.addScenarioComparison(comparison);

    String serialize = query.json();
    ScenarioGroupingQueryDto deserialize = JacksonUtil.deserialize(serialize, ScenarioGroupingQueryDto.class);
    Assertions.assertThat(deserialize.table).isEqualTo(query.table);
    Assertions.assertThat(deserialize.comparisons).isEqualTo(query.comparisons);
    Assertions.assertThat(deserialize.groups).isEqualTo(query.groups);
  }
}
