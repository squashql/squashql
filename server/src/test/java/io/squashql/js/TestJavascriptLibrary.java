package io.squashql.js;

import static io.squashql.query.Functions.all;
import static io.squashql.query.Functions.and;
import static io.squashql.query.Functions.avg;
import static io.squashql.query.Functions.avgIf;
import static io.squashql.query.Functions.criterion;
import static io.squashql.query.Functions.decimal;
import static io.squashql.query.Functions.divide;
import static io.squashql.query.Functions.eq;
import static io.squashql.query.Functions.ge;
import static io.squashql.query.Functions.gt;
import static io.squashql.query.Functions.in;
import static io.squashql.query.Functions.integer;
import static io.squashql.query.Functions.isNotNull;
import static io.squashql.query.Functions.isNull;
import static io.squashql.query.Functions.like;
import static io.squashql.query.Functions.lt;
import static io.squashql.query.Functions.or;
import static io.squashql.query.Functions.plus;
import static io.squashql.query.Functions.sum;
import static io.squashql.query.TableField.tableField;
import static io.squashql.query.TableField.tableFields;

import io.squashql.jackson.JacksonUtil;
import io.squashql.query.AggregatedMeasure;
import io.squashql.query.BasicMeasure;
import io.squashql.query.BinaryOperationMeasure;
import io.squashql.query.BinaryOperator;
import io.squashql.query.ColumnSetKey;
import io.squashql.query.ComparisonMeasureReferencePosition;
import io.squashql.query.ComparisonMethod;
import io.squashql.query.ConstantField;
import io.squashql.query.CountMeasure;
import io.squashql.query.ExpressionMeasure;
import io.squashql.query.Measure;
import io.squashql.query.TableField;
import io.squashql.query.TotalCountMeasure;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.BucketColumnSetDto;
import io.squashql.query.dto.ConditionType;
import io.squashql.query.dto.JoinMappingDto;
import io.squashql.query.dto.JoinType;
import io.squashql.query.dto.OrderKeywordDto;
import io.squashql.query.dto.Period;
import io.squashql.query.dto.PivotTableQueryDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.QueryMergeDto;
import io.squashql.query.dto.TableDto;
import io.squashql.query.dto.VirtualTableDto;
import io.squashql.query.parameter.QueryCacheParameter;
import io.squashql.util.TestUtil;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJavascriptLibrary {

  /**
   * This is test is building the {@link QueryDto} wo. the help of the query builder {@link Query}.
   */
  @Test
  void testReadJsonBuildFromQueryDto() {
    var table = new TableDto("myTable");
    var refTable = new TableDto("refTable");
    table.join(refTable, JoinType.INNER, new JoinMappingDto("fromField", "toField"));
    table.join(new TableDto("a"), JoinType.LEFT, new JoinMappingDto("a" + ".a_id", "myTable" + ".id"));

    QueryDto q = new QueryDto()
            .table(table)
            .withColumn(tableField("a"))
            .withColumn(tableField("b"));

    var price = new AggregatedMeasure("price.sum", "price", "sum");
    q.withMeasure(price);
    var priceFood = new AggregatedMeasure("alias", "price", "sum", criterion("category", eq("food")));
    q.withMeasure(priceFood);
    var plus = new BinaryOperationMeasure("plusMeasure", BinaryOperator.PLUS, price, priceFood);
    q.withMeasure(plus);
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
    q.withMeasure(avgIf("whatever", divide(f1, plus(one, rate)), criterion(plus(f1, f2), one, ConditionType.GT)));

    q.withMeasure(new ComparisonMeasureReferencePosition("comp bucket",
            ComparisonMethod.ABSOLUTE_DIFFERENCE,
            price,
            Map.of("scenario", "s-1", "group", "g"),
            ColumnSetKey.BUCKET));

    Period.Month month = new Period.Month("mois", "annee");
    q.withMeasure(new ComparisonMeasureReferencePosition("growth",
            ComparisonMethod.DIVIDE,
            price,
            Map.of("Annee", "y-1", "Mois", "m"),
            month));

    q.withMeasure(new ComparisonMeasureReferencePosition("parent",
            ComparisonMethod.DIVIDE,
            price,
            List.of("Mois", "Annee")));

    var queryCondition = or(and(eq("a"), eq("b")), lt(5), like("a%"));
    q.withCondition(tableField("f1"), queryCondition);
    q.withCondition(tableField("f2"), gt(659));
    q.withCondition(tableField("f3"), in(0, 1, 2));
    q.withCondition(tableField("f4"), isNull());
    q.withCondition(tableField("f5"), isNotNull());

    q.withHavingCriteria(all(criterion(price, ge(10)), criterion(expression, lt(100))));

    q.orderBy(tableField("a"), OrderKeywordDto.ASC);
    q.orderBy(tableField("b"), List.of("1", "l", "p"));

    BucketColumnSetDto columnSet = new BucketColumnSetDto("group", "scenario")
            .withNewBucket("a", List.of("a1", "a2"))
            .withNewBucket("b", List.of("b1", "b2"));
    q.withColumnSet(ColumnSetKey.BUCKET, columnSet);

    QueryDto subQuery = new QueryDto()
            .table(table)
            .withColumn(tableField("aa"))
            .withMeasure(sum("sum_aa", "f"));
    q.table(subQuery);

    String name = "build-from-querydto.json"; // The content of this file is generated by the js code.
    QueryDto qjs = JacksonUtil.deserialize(TestUtil.readAllLines(name), QueryDto.class);
    Assertions.assertThat(q.columnSets).isEqualTo(qjs.columnSets);
    Assertions.assertThat(q.columns).isEqualTo(qjs.columns);
    Assertions.assertThat(q.rollupColumns).isEqualTo(qjs.rollupColumns);
    Assertions.assertThat(q.parameters).isEqualTo(qjs.parameters);
    Assertions.assertThat(q.orders).isEqualTo(qjs.orders);
    Assertions.assertThat(q.measures).isEqualTo(qjs.measures);
    Assertions.assertThat(q.whereCriteriaDto).isEqualTo(qjs.whereCriteriaDto);
    Assertions.assertThat(q.table).isEqualTo(qjs.table);
    Assertions.assertThat(q.subQuery).isEqualTo(qjs.subQuery);
    Assertions.assertThat(q).isEqualTo(qjs);
  }


  /**
   * This is test is building the {@link QueryDto} <b>with</b> by using the query builder {@link Query}.
   */
  @Test
  void testReadJsonBuildFromQuery() {
    var table = new TableDto("myTable");
    var refTable = new TableDto("refTable");
    var cte = new VirtualTableDto("myCte", List.of("id", "min", "max", "other"), List.of(List.of(0, 0, 1, "x"), List.of(1, 2, 3, "y")));

    BucketColumnSetDto bucketColumnSet = new BucketColumnSetDto("group", "scenario")
            .withNewBucket("a", List.of("a1", "a2"))
            .withNewBucket("b", List.of("b1", "b2"));

    Measure measure = sum("sum", "f1");
    Measure measureExpr = new ExpressionMeasure("sum_expr", "sum(f1)");
    QueryDto q = Query.from(table.name)
            .join(refTable.name, JoinType.INNER)
            .on(all(criterion("myTable" + ".id", "refTable" + ".id", ConditionType.EQ),
                    criterion("myTable" + ".a", "refTable" + ".a", ConditionType.EQ)))
            .join(cte, JoinType.INNER)
            .on(all(criterion("myTable.value", "myCte.min", ConditionType.GE), criterion("myTable.value", "myCte.max", ConditionType.LT)))
            .where(tableField("f2"), gt(659))
            .where(tableField("f3"), eq(123))
            .select(tableFields(List.of("a", "b")),
                    List.of(bucketColumnSet),
                    List.of(measure, avg("sum", "f1"), measureExpr))
            .rollup(tableField("a"), tableField("b"))
            .having(all(criterion((BasicMeasure) measure, gt(0)), criterion((BasicMeasure) measureExpr, lt(10))))
            .orderBy(tableField("f4"), OrderKeywordDto.ASC)
            .limit(10)
            .build();

    q.withParameter(QueryCacheParameter.KEY, new QueryCacheParameter(QueryCacheParameter.Action.NOT_USE));

    String name = "build-from-query.json"; // The content of this file is generated by the js code.
    QueryDto qjs = JacksonUtil.deserialize(TestUtil.readAllLines(name), QueryDto.class);
    Assertions.assertThat(q.columnSets).isEqualTo(qjs.columnSets);
    Assertions.assertThat(q.columns).isEqualTo(qjs.columns);
    Assertions.assertThat(q.rollupColumns).isEqualTo(qjs.rollupColumns);
    Assertions.assertThat(q.parameters).isEqualTo(qjs.parameters);
    Assertions.assertThat(q.orders).isEqualTo(qjs.orders);
    Assertions.assertThat(q.measures).isEqualTo(qjs.measures);
    Assertions.assertThat(q.whereCriteriaDto).isEqualTo(qjs.whereCriteriaDto);
    Assertions.assertThat(q.table).isEqualTo(qjs.table);
    Assertions.assertThat(q.subQuery).isEqualTo(qjs.subQuery);
    Assertions.assertThat(q.limit).isEqualTo(qjs.limit);
    Assertions.assertThat(q.virtualTableDto).isEqualTo(qjs.virtualTableDto);
    Assertions.assertThat(q).isEqualTo(qjs);
  }

  @Test
  void testReadJsonBuildFromQueryMerge() {
    var table = new TableDto("myTable");
    QueryDto q1 = Query.from(table.name)
            .select(tableFields(List.of("a", "b")),
                    List.of(sum("sum", "f1")))
            .build();
    QueryDto q2 = Query.from(table.name)
            .select(tableFields(List.of("a", "b")),
                    List.of(avg("sum", "f1")))
            .build();

    QueryMergeDto query = new QueryMergeDto(q1, q2, JoinType.LEFT);

    String name = "build-from-query-merge.json"; // The content of this file is generated by the js code.
    QueryMergeDto qjs = JacksonUtil.deserialize(TestUtil.readAllLines(name), QueryMergeDto.class);
    Assertions.assertThat(qjs).isEqualTo(query);
  }

  @Test
  void testReadJsonBuildFromQueryPivot() {
    var table = new TableDto("myTable");
    QueryDto q = Query.from(table.name)
            .select(tableFields(List.of("a", "b")),
                    List.of(avg("sum", "f1")))
            .build();

    String name = "build-from-query-pivot.json"; // The content of this file is generated by the js code.
    PivotTableQueryDto qjs = JacksonUtil.deserialize(TestUtil.readAllLines(name), PivotTableQueryDto.class);
    Assertions.assertThat(qjs).isEqualTo(new PivotTableQueryDto(q, List.of("a"), List.of("b")));
  }
}
