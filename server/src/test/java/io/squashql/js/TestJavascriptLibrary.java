package io.squashql.js;

import io.squashql.jackson.JacksonUtil;
import io.squashql.query.*;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.*;
import io.squashql.query.measure.ParametrizedMeasure;
import io.squashql.query.measure.Repository;
import io.squashql.query.parameter.QueryCacheParameter;
import io.squashql.util.TestUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.squashql.query.Functions.*;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;
import static io.squashql.query.builder.Query.from;

public class TestJavascriptLibrary {

  /**
   * This is test is building the {@link QueryDto} wo. the help of the query builder {@link Query}.
   */
  @Test
  void testReadJsonBuildFromQueryDto() {
    var table = new TableDto("myTable");
    var refTable = new TableDto("refTable");
    table.join(refTable, JoinType.INNER, criterion("fromField", "toField", ConditionType.EQ));
    table.join(new TableDto("a"), JoinType.LEFT, criterion("a" + ".a_id", "myTable" + ".id", ConditionType.EQ));

    Field a = tableField("a");
    Field b = tableField("b").as("b_alias");
    QueryDto q = new QueryDto()
            .table(table)
            .withColumn(a)
            .withColumn(b);

    var price = new AggregatedMeasure("price.sum", "price", "sum");
    q.withMeasure(price);
    var priceFood = new AggregatedMeasure("alias", "price", "sum", criterion("category", eq("food")));
    q.withMeasure(priceFood);
    var plus = new BinaryOperationMeasure("plusMeasure", BinaryOperator.PLUS, price, priceFood);
    q.withMeasure(plus);
    var relDiff = new BinaryOperationMeasure("relDiff", BinaryOperator.RELATIVE_DIFFERENCE, price, priceFood);
    q.withMeasure(relDiff);
    var expression = new ExpressionMeasure("myExpression", "sum(price*quantity)");
    q.withMeasure(expression);
    q.withMeasure(CountMeasure.INSTANCE);
    q.withMeasure(TotalCountMeasure.INSTANCE);
    q.withMeasure(integer(123));
    q.withMeasure(decimal(1.23));

    var f1 = new TableField("myTable.f1");
    var f2 = new TableField("myTable.f2");
    var rate = new TableField("rate");
    var one = new ConstantField(1);
    q.withMeasure(avgIf("whatever", divide(f1, plus(one, rate)), criterion(plus(f1, f2).as("f1+f2"), one, ConditionType.GT)));

    q.withMeasure(new ComparisonMeasureReferencePosition("comp group",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of(tableField("scenario"), "s-1", tableField("group"), "g"),
            ColumnSetKey.GROUP));

    Period.Month month = new Period.Month(tableField("Month"), tableField("Year"));
    q.withMeasure(new ComparisonMeasureReferencePosition("growth",
            ComparisonMethod.DIVIDE,
            price,
            Map.of(tableField("Year"), "y-1", tableField("Month"), "m"),
            month));

    q.withMeasure(new ComparisonMeasureReferencePosition("parent",
            ComparisonMethod.DIVIDE,
            price,
            List.of(tableField("Year"), tableField("Month"))));
    q.withMeasure(new ComparisonMeasureReferencePosition("grandTotalAlongAncestors",
            ComparisonMethod.DIVIDE,
            price,
            List.of(tableField("Year"), tableField("Month")),
            true));
    q.withMeasure(new ComparisonMeasureGrandTotal("grandTotal",
            ComparisonMethod.DIVIDE,
            price));
    q.withMeasure(new ParametrizedMeasure("var measure", Repository.VAR, Map.of(
            "value", tableField("price"),
            "date", tableField("date"),
            "quantile", 0.95
    )));
    q.withMeasure(new ParametrizedMeasure("incr var measure", Repository.INCREMENTAL_VAR, Map.of(
            "value", tableField("price"),
            "date", tableField("date"),
            "quantile", 0.95,
            "ancestors", tableFields(List.of("f1", "f2", "f3"))
    )));

    var queryCondition = or(and(eq("a"), eq("b")), lt(5), like("a%"));
    q.withCondition(tableField("f1"), queryCondition);
    q.withCondition(tableField("f2"), gt(659));
    q.withCondition(tableField("f3"), in(0, 1, 2));
    q.withCondition(tableField("f4"), isNull());
    q.withCondition(tableField("f5"), isNotNull());
    q.withCondition(tableField("f6"), contains(2));

    q.withHavingCriteria(all(criterion(price, ge(10)), criterion(expression, lt(100))));

    q.orderBy(a, OrderKeywordDto.ASC);
    q.orderBy(b, List.of("1", "l", "p"));

    GroupColumnSetDto columnSet = new GroupColumnSetDto("group", tableField("scenario"))
            .withNewGroup("a", List.of("a1", "a2"))
            .withNewGroup("b", List.of("b1", "b2"));
    q.withColumnSet(ColumnSetKey.GROUP, columnSet);

    QueryDto subQuery = new QueryDto()
            .table(table)
            .withColumn(tableField("aa"))
            .withColumn(new AliasedField("bb"))
            .withMeasure(sum("sum_aa", "f"));
    q.table(subQuery);

    String name = "json/build-from-querydto.json"; // The content of this file is generated by the js code.
    QueryDto qjs = JacksonUtil.deserialize(TestUtil.readAllLines(name), QueryDto.class);
    Assertions.assertThat(q.columnSets).isEqualTo(qjs.columnSets);
    Assertions.assertThat(q.columns).isEqualTo(qjs.columns);
    Assertions.assertThat(q.rollupColumns).isEqualTo(qjs.rollupColumns);
    Assertions.assertThat(q.parameters).isEqualTo(qjs.parameters);
    Assertions.assertThat(q.orders).isEqualTo(qjs.orders);
    Assertions.assertThat(q.measures).isEqualTo(qjs.measures);
    Assertions.assertThat(q.whereCriteriaDto).isEqualTo(qjs.whereCriteriaDto);
    Assertions.assertThat(q.table).isEqualTo(qjs.table);
    Assertions.assertThat(q).isEqualTo(qjs);
  }

  /**
   * This is test is building the {@link QueryDto} <b>with</b> by using the query builder {@link Query}.
   */
  @Test
  void testReadJsonBuildFromQuery() {
    var table = new TableDto("myTable");
    var refTable = new TableDto("refTable");
    var cte1 = new VirtualTableDto("myCte1", List.of("id", "min", "max", "other"), List.of(List.of(0, 0, 1, "x"), List.of(1, 2, 3, "y")));
    var cte2 = new VirtualTableDto("myCte2", List.of("id", "min", "max", "other"), List.of(List.of(0, 4, 12, "a"), List.of(1, 12, 25, "b")));

    GroupColumnSetDto groupColumnSet = new GroupColumnSetDto("group", tableField("scenario"))
            .withNewGroup("a", List.of("a1", "a2"))
            .withNewGroup("b", List.of("b1", "b2"));

    Measure measure = sum("sum", "f1");
    Measure measureExpr = new ExpressionMeasure("sum_expr", "sum(f1)");
    QueryDto q = from(table.name)
            .join(refTable.name, JoinType.INNER)
            .on(all(criterion("myTable" + ".id", "refTable" + ".id", ConditionType.EQ),
                    criterion("myTable" + ".a", "refTable" + ".a", ConditionType.EQ)))
            .join(cte1, JoinType.INNER)
            .on(all(criterion("myTable.value", "myCte1.min", ConditionType.GE), criterion("myTable.value", "myCte1.max", ConditionType.LT)))
            .join(cte2, JoinType.INNER)
            .on(all(criterion("myTable.value", "myCte2.min", ConditionType.GE), criterion("myTable.value", "myCte2.max", ConditionType.LT)))
            .where(tableField("f2"), gt(659))
            .where(tableField("f3"), eq(123))
            .select(tableFields(List.of("a", "b")),
                    List.of(groupColumnSet),
                    List.of(measure, avg("sum", "f1"), measureExpr))
            .rollup(tableField("a"), tableField("b"))
            .having(all(criterion((BasicMeasure) measure, gt(0)), criterion((BasicMeasure) measureExpr, lt(10))))
            .orderBy(tableField("f4"), OrderKeywordDto.ASC)
            .limit(10)
            .build();

    q.withParameter(QueryCacheParameter.KEY, new QueryCacheParameter(QueryCacheParameter.Action.NOT_USE));
    q.minify = false;

    String name = "json/build-from-query.json"; // The content of this file is generated by the js code.
    QueryDto qjs = JacksonUtil.deserialize(TestUtil.readAllLines(name), QueryDto.class);
    Assertions.assertThat(q.columnSets).isEqualTo(qjs.columnSets);
    Assertions.assertThat(q.columns).isEqualTo(qjs.columns);
    Assertions.assertThat(q.rollupColumns).isEqualTo(qjs.rollupColumns);
    Assertions.assertThat(q.parameters).isEqualTo(qjs.parameters);
    Assertions.assertThat(q.orders).isEqualTo(qjs.orders);
    Assertions.assertThat(q.measures).isEqualTo(qjs.measures);
    Assertions.assertThat(q.whereCriteriaDto).isEqualTo(qjs.whereCriteriaDto);
    Assertions.assertThat(q.table).isEqualTo(qjs.table);
    Assertions.assertThat(q.limit).isEqualTo(qjs.limit);
    Assertions.assertThat(q.virtualTableDtos).isEqualTo(qjs.virtualTableDtos);
    Assertions.assertThat(q.minify).isEqualTo(qjs.minify);
    Assertions.assertThat(q).isEqualTo(qjs);
  }

  /**
   * This is test is building the {@link QueryDto} <b>with</b> by using the query builder {@link Query}.
   */
  @Test
  void testReadJsonBuildFromSubquery() {
    var cte1 = new VirtualTableDto("myCte1", List.of("id", "other"), List.of(List.of(0, "x"), List.of(1, "y")));

    var sq = from("myTable")
            .select(tableFields(List.of("myTable.a", "myTable.b")), List.of(sum("pnl_sum", new TableField("myTable.pnl"))))
            .build();

    QueryDto q = from(sq)
            .join(cte1, JoinType.INNER)
            .on(criterion("myTable.a", "myCte1.id", ConditionType.EQ))
            .select(tableFields(List.of("myTable.a", "myCte1.other")),
                    List.of(avg("pnl_avg", new AliasedField("pnl_sum"))))
            .build();

    String name = "json/build-from-subquery.json"; // The content of this file is generated by the js code.
    QueryDto qjs = JacksonUtil.deserialize(TestUtil.readAllLines(name), QueryDto.class);
    Assertions.assertThat(q).isEqualTo(qjs);
  }

  @Test
  void testReadJsonBuildFromQueryMerge() {
    var table = new TableDto("myTable");
    QueryDto q1 = from(table.name)
            .select(tableFields(List.of("a", "b")),
                    List.of(sum("sum", "f1")))
            .build();
    QueryDto q2 = from(table.name)
            .select(tableFields(List.of("a", "b")),
                    List.of(avg("avg", "f1")))
            .build();

    QueryDto q3 = from(table.name)
            .select(tableFields(List.of("a", "b")),
                    List.of(max("max", "f1")))
            .build();

    QueryMergeDto query = QueryMergeDto.from(q1).join(q2, JoinType.LEFT).join(q3, JoinType.INNER).withParameter(new QueryCacheParameter(QueryCacheParameter.Action.NOT_USE));
    String name = "json/build-from-query-merge.json"; // The content of this file is generated by the js code.
    QueryMergeDto qjs = JacksonUtil.deserialize(TestUtil.readAllLines(name), QueryMergeDto.class);
    Assertions.assertThat(qjs).isEqualTo(query);
  }

  @Test
  void testReadJsonBuildFromQueryPivot() {
    var table = new TableDto("myTable");
    QueryDto q = from(table.name)
            .select(tableFields(List.of("a", "b")),
                    List.of(avg("sum", "f1")))
            .build();
    q.minify = true;

    String name = "json/build-from-query-pivot.json"; // The content of this file is generated by the js code.
    PivotTableQueryDto qjs = JacksonUtil.deserialize(TestUtil.readAllLines(name), PivotTableQueryDto.class);
    Assertions.assertThat(qjs).isEqualTo(new PivotTableQueryDto(q, tableFields(List.of("a")), tableFields(List.of("b")), tableFields(List.of("b"))));
  }

  @Test
  void testReadJsonBuildFromQueryMergePivot() {
    var table = new TableDto("myTable");
    QueryDto q1 = from(table.name)
            .select(tableFields(List.of("a", "b")),
                    List.of(sum("sum", "f1")))
            .build();
    QueryDto q2 = from(table.name)
            .select(tableFields(List.of("a", "b")),
                    List.of(avg("sum", "f1")))
            .build();

    QueryMergeDto query = QueryMergeDto.from(q1).join(q2, JoinType.LEFT);
    query.minify = false;
    PivotTableQueryMergeDto pivotTableQueryMergeDto = new PivotTableQueryMergeDto(query, tableFields(List.of("a")), tableFields(List.of("b")));

    String name = "json/build-from-query-merge-pivot.json"; // The content of this file is generated by the js code.
    PivotTableQueryMergeDto qjs = JacksonUtil.deserialize(TestUtil.readAllLines(name), PivotTableQueryMergeDto.class);
    Assertions.assertThat(qjs).isEqualTo(pivotTableQueryMergeDto);
  }

  @Test
  void testReadJsonBuildFromQueryJoin() {
    var a = new TableField("myTable1", "a");
    var b1 = new TableField("myTable1", "b");
    var c1 = new TableField("myTable1", "c");
    var query1 = from("myTable1")
            .select(List.of(a, b1, c1), List.of(sum("sum", new TableField("f1"))))
            .build();

    var b2 = new TableField("myTable2", "b");
    var c2 = new TableField("myTable2", "c");
    var query2 = from("myTable2")
            .select(List.of(b2, c2), List.of(avg("sum", new TableField("f2"))))
            .build();

    var c3 = new TableField("myTable3.c3");
    var query3 = from("myTable3")
            .select(List.of(c3), List.of(avg("max", new TableField("f3"))))
            .build();

    QueryJoinDto q = QueryJoinDto.from(query1)
            .join(query2,
                    JoinType.LEFT,
                    all(
                            criterion(b1, b2, ConditionType.EQ),
                            criterion(c1, c2, ConditionType.EQ)))
            .orderBy(Map.of(a, new SimpleOrderDto(OrderKeywordDto.ASC), c3, new SimpleOrderDto(OrderKeywordDto.DESC)))
            .join(query3, JoinType.INNER)
            .limit(12);

    q.current = 0;
    q.minify = true;

    String name = "json/build-from-query-join.json"; // The content of this file is generated by the js code.
    QueryJoinDto qjs = JacksonUtil.deserialize(TestUtil.readAllLines(name), QueryJoinDto.class);
    Assertions.assertThat(qjs).isEqualTo(q);
  }

  @Test
  void testQueryResult() {
    String name = "json/build-from-query-result.json"; // The content of this file is generated by the js code.
    QueryResultDto expected = new QueryResultDto();
    expected.columns = List.of("key1", "key2");
    expected.cells = List.of(
            Map.of("key1", 1, "key2", "sthg"),
            Map.of("key1", 2, "key2", "sthg else")
    );
    expected.metadata = List.of(
            new MetadataItem("key1", "key1", int.class),
            new MetadataItem("key2", "key2", String.class)
    );
    QueryResultDto r = JacksonUtil.deserialize(TestUtil.readAllLines(name), QueryResultDto.class);
    Assertions.assertThat(r).isEqualTo(expected);
  }

  @Test
  void testPivotQueryResult() {
    String name = "json/build-from-pivot-query-result.json"; // The content of this file is generated by the js code.
    PivotTableQueryResultDto expected = new PivotTableQueryResultDto();
    expected.cells = List.of(
            Map.of("key1", 1, "key2", "sthg", "key3", true, "key4", 123.5),
            Map.of("key1", 2, "key2", "sthg else", "key3", false, "key4", 321.5)
    );
    expected.rows = List.of("key1", "key2");
    expected.columns = List.of("key3");
    expected.values = List.of("key4");
    expected.hiddenTotals = List.of("key2");
    PivotTableQueryResultDto r = JacksonUtil.deserialize(TestUtil.readAllLines(name), PivotTableQueryResultDto.class);
    Assertions.assertThat(r).isEqualTo(expected);
  }
}
