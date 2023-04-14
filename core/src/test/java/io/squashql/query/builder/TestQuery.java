package io.squashql.query.builder;

import io.squashql.query.AggregatedMeasure;
import io.squashql.query.ColumnSetKey;
import io.squashql.query.Measure;
import io.squashql.query.dto.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.squashql.query.Functions.*;

public class TestQuery {

  @Test
  void testSimple() {
    BucketColumnSetDto columnSet = new BucketColumnSetDto();
    Measure sum = sum("sum", "f2");

    QueryDto build = Query
            .from("saas")
            .select(List.of("col1", "col2"), List.of(columnSet), List.of(sum))
            .build();

    QueryDto q = new QueryDto()
            .table("saas")
            .withColumn("col1")
            .withColumn("col2")
            .withColumnSet(ColumnSetKey.BUCKET, columnSet)
            .withMeasure(sum);

    Assertions.assertThat(build).isEqualTo(q);

    // Only one condition
    build = Query
            .from("saas")
            .where("f1", eq("A"))
            .select(List.of("col1", "col2"), List.of(columnSet), List.of(sum))
            .build();

    Assertions.assertThat(build).isEqualTo(q.withCondition("f1", eq("A")));

    // Multiple conditions
    CriteriaDto any = any(criterion("f3", eq("C")), criterion("f3", isNull()));
    build = Query
            .from("saas")
            .where("f1", eq("A"))
            .where("f2", eq("B"))
            .where(any)
            .select(List.of("col1", "col2"), List.of(columnSet), List.of(sum))
            .build();

    Assertions.assertThat(build).isEqualTo(q.withCondition("f2", eq("B")).withWhereCriteria(any));

    // with limit
    build = Query
            .from("saas")
            .select(List.of("col1", "col2"), List.of(columnSet), List.of(sum))
            .limit(100)
            .build();

    q = new QueryDto()
            .table("saas")
            .withColumn("col1")
            .withColumn("col2")
            .withColumnSet(ColumnSetKey.BUCKET, columnSet)
            .withMeasure(sum)
            .withLimit(100);

    Assertions.assertThat(build).isEqualTo(q);
  }

  @Test
  void testWithSingleJoin() {
    Measure sum = sum("sum", "f2");
    {
      TableDto saas = new TableDto("saas");
      TableDto other = new TableDto("other");

      QueryDto build = Query
              .from("saas")
              .leftOuterJoin("other")
              .on(criterion(other.name + ".id", saas.name + ".id", ConditionType.EQ))
              .select(List.of("col1", "col2"), List.of(sum))
              .build();

      saas.join(other, JoinType.LEFT, new JoinMappingDto(other.name + ".id", saas.name + ".id"));

      QueryDto q = new QueryDto()
              .table(saas)
              .withColumn("col1")
              .withColumn("col2")
              .withMeasure(sum);

      Assertions.assertThat(build).isEqualTo(q);
    }

    {
      TableDto saas = new TableDto("saas");
      TableDto other = new TableDto("other");

      saas.join(other, JoinType.INNER, List.of(
              new JoinMappingDto(other.name + ".id", saas.name + ".id"),
              new JoinMappingDto(other.name + ".a", saas.name + ".b")));

      QueryDto q = new QueryDto()
              .table(saas)
              .withColumn("col1")
              .withColumn("col2")
              .withMeasure(sum);

      // With two join conditions
      QueryDto build = Query
              .from("saas")
              .innerJoin("other")
              .on(all(
                      criterion(other.name + ".id", saas.name + ".id", ConditionType.EQ),
                      criterion(other.name + ".a", saas.name + ".b", ConditionType.EQ)
              ))
              .select(List.of("col1", "col2"), List.of(sum))
              .build();

      Assertions.assertThat(build).isEqualTo(q);
    }

    {
      TableDto saas = new TableDto("saas");
      TableDto other = new TableDto("other");

      QueryDto q = new QueryDto()
              .table(saas)
              .withColumn("col1")
              .withColumn("col2")
              .withMeasure(sum)
              .withCondition("f1", eq("A"));

      saas.join(other, JoinType.INNER, new JoinMappingDto(other.name + ".id", saas.name + ".id"));

      // With condition on the "joined" table
      QueryDto build = Query
              .from("saas")
              .innerJoin("other")
              .on(criterion(other.name + ".id", saas.name + ".id", ConditionType.EQ))
              .where("f1", eq("A"))
              .select(List.of("col1", "col2"), List.of(sum))
              .build();

      Assertions.assertThat(build).isEqualTo(q);
    }
  }

  @Test
  void testWithMultipleJoins() {
    TableDto saas = new TableDto("saas");
    TableDto other = new TableDto("other");
    TableDto another = new TableDto("another");
    Measure sum = sum("sum", "f2");

    QueryDto q = new QueryDto()
            .table(saas)
            .withColumn("col1")
            .withColumn("col2")
            .withMeasure(sum);

    saas.join(other, JoinType.LEFT, new JoinMappingDto(other.name + ".id", saas.name + ".id"));
    saas.join(another, JoinType.INNER, new JoinMappingDto(another.name + ".id", saas.name + ".id"));

    QueryDto build = Query
            .from("saas")
            .leftOuterJoin("other")
            .on(criterion(other.name + ".id", saas.name + ".id", ConditionType.EQ))
            .innerJoin("another")
            .on(criterion(another.name + ".id", saas.name + ".id", ConditionType.EQ))
            .select(List.of("col1", "col2"), List.of(sum))
            .build();

    Assertions.assertThat(build).isEqualTo(q);
  }

  @Test
  void testJoinWithVirtualTable() {
    Measure sum = sum("sum", "f2");
    TableDto saas = new TableDto("saas");
    VirtualTableDto vt = new VirtualTableDto("vtable", List.of("id", "c"), List.of(List.of("k", "c")));
    QueryDto build = Query
            .from("saas")
            .innerJoin(vt)
            .on(criterion("saas.id", "vtable.id", ConditionType.EQ))
            .select(List.of("col1", "col2"), List.of(sum))
            .build();

    saas.join(new TableDto("vtable"), JoinType.INNER, new JoinMappingDto(saas.name + ".id", vt.name + ".id"));

    QueryDto q = new QueryDto()
            .table(saas)
            .withColumn("col1")
            .withColumn("col2")
            .withMeasure(sum);
    q.virtualTableDto = vt;

    Assertions.assertThat(build).isEqualTo(q);
  }

  @Test
  void testOrderBy() {
    Measure sum = sum("sum", "f2");

    // Single order by
    QueryDto build = Query
            .from("saas")
            .select(List.of("col1", "col2"), List.of(sum))
            .orderBy("col1", OrderKeywordDto.ASC)
            .build();

    QueryDto q = new QueryDto()
            .table("saas")
            .withColumn("col1")
            .withColumn("col2")
            .orderBy("col1", OrderKeywordDto.ASC)
            .withMeasure(sum);

    Assertions.assertThat(build).isEqualTo(q);

    // Multiple orders by
    build = Query
            .from("saas")
            .select(List.of("col1", "col2"), List.of(sum))
            .orderBy("col1", OrderKeywordDto.ASC)
            .orderBy("col2", List.of("1", "10"))
            .build();

    q = new QueryDto()
            .table("saas")
            .withColumn("col1")
            .withColumn("col2")
            .orderBy("col1", OrderKeywordDto.ASC)
            .orderBy("col2", List.of("1", "10"))
            .withMeasure(sum);

    Assertions.assertThat(build).isEqualTo(q);
  }

  @Test
  void testOrderByWithLimit() {
    Measure sum = sum("sum", "f2");

    // Single order by
    QueryDto build = Query
            .from("saas")
            .select(List.of("col1", "col2"), List.of(sum))
            .orderBy("col1", OrderKeywordDto.ASC)
            .limit(10)
            .build();

    QueryDto q = new QueryDto()
            .table("saas")
            .withColumn("col1")
            .withColumn("col2")
            .orderBy("col1", OrderKeywordDto.ASC)
            .withLimit(10)
            .withMeasure(sum);

    Assertions.assertThat(build).isEqualTo(q);
  }

  @Test
  void testRollup() {
    Measure sum = sum("sum", "f2");
    QueryDto build = Query
            .from("saas")
            .select(List.of("col1", "col2"), List.of(sum))
            .rollup("col1")
            .orderBy("col1", OrderKeywordDto.ASC)
            .build();

    QueryDto q = new QueryDto()
            .table("saas")
            .withColumn("col1")
            .withColumn("col2")
            .withRollup("col1")
            .orderBy("col1", OrderKeywordDto.ASC)
            .withMeasure(sum);

    Assertions.assertThat(build).isEqualTo(q);
  }

  @Test
  void testHaving() {
    Measure sum = sum("sum", "f2");
    CriteriaDto criterion = criterion((AggregatedMeasure) sum, ge(0));
    QueryDto buildWoRollup = Query
            .from("saas")
            .select(List.of("col1"), List.of(sum))
            .having(criterion)
            .build();

    QueryDto buildWithRollup = Query
            .from("saas")
            .select(List.of("col1"), List.of(sum))
            .rollup(List.of("col1"))
            .having(criterion)
            .build();

    QueryDto qWithRollup = new QueryDto()
            .table("saas")
            .withColumn("col1")
            .withRollup("col1")
            .withHavingCriteria(criterion)
            .withMeasure(sum);

    QueryDto qWoRollup = new QueryDto()
            .table("saas")
            .withColumn("col1")
            .withHavingCriteria(criterion)
            .withMeasure(sum);

    Assertions.assertThat(buildWoRollup).isEqualTo(qWoRollup);
    Assertions.assertThat(buildWithRollup).isEqualTo(qWithRollup);
  }
}
