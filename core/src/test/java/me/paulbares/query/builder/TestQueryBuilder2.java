package me.paulbares.query.builder;

import me.paulbares.query.ColumnSet;
import me.paulbares.query.ColumnSetKey;
import me.paulbares.query.Measure;
import me.paulbares.query.QueryBuilder;
import me.paulbares.query.dto.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static me.paulbares.query.QueryBuilder.sum;

public class TestQueryBuilder2 {

  @Test
  void testSimple() {
    ColumnSet year = QueryBuilder.createPeriodColumnSet(new Period.Year("Year"));
    Measure sum = sum("sum", "f2");

    QueryDto build = QueryBuilder2
            .from("saas")
            .select(List.of("col1", "col2"), List.of(year), List.of(sum))
            .build();

    QueryDto q = new QueryDto()
            .table("saas")
            .withColumn("col1")
            .withColumn("col2")
            .withColumnSet(ColumnSetKey.PERIOD, year)
            .withMeasure(sum);

    Assertions.assertThat(build).isEqualTo(q);

    // Only one condition
    build = QueryBuilder2
            .from("saas")
            .where("f1", QueryBuilder.eq("A"))
            .select(List.of("col1", "col2"), List.of(year), List.of(sum))
            .build();

    Assertions.assertThat(build).isEqualTo(q.withCondition("f1", QueryBuilder.eq("A")));

    // Multiple conditions
    build = QueryBuilder2
            .from("saas")
            .where("f1", QueryBuilder.eq("A"))
            .where("f2", QueryBuilder.eq("B"))
            .select(List.of("col1", "col2"), List.of(year), List.of(sum))
            .build();

    Assertions.assertThat(build).isEqualTo(q.withCondition("f2", QueryBuilder.eq("B")));

    // with limit
    build = QueryBuilder2
            .from("saas")
            .select(List.of("col1", "col2"), List.of(year), List.of(sum))
            .limit(100)
            .build();

    q = new QueryDto()
            .table("saas")
            .withColumn("col1")
            .withColumn("col2")
            .withColumnSet(ColumnSetKey.PERIOD, year)
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

      QueryDto build = QueryBuilder2
              .from("saas")
              .leftOuterJoin("other")
              .on(other.name, "id", saas.name, "id")
              .select(List.of("col1", "col2"), List.of(sum))
              .build();

      saas.join(other, JoinType.LEFT, new JoinMappingDto(other.name, "id", saas.name, "id"));

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
              new JoinMappingDto(other.name, "id", saas.name, "id"),
              new JoinMappingDto(other.name, "a", saas.name, "b")));

      QueryDto q = new QueryDto()
              .table(saas)
              .withColumn("col1")
              .withColumn("col2")
              .withMeasure(sum);

      // With two join conditions
      QueryDto build = QueryBuilder2
              .from("saas")
              .innerJoin("other")
              .on(other.name, "id", saas.name, "id")
              .on(other.name, "a", saas.name, "b")
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
              .withCondition("f1", QueryBuilder.eq("A"));

      saas.join(other, JoinType.INNER, new JoinMappingDto(other.name, "id", saas.name, "id"));

      // With condition on the "joined" table
      QueryDto build = QueryBuilder2
              .from("saas")
              .innerJoin("other")
              .on(other.name, "id", saas.name, "id")
              .where("f1", QueryBuilder.eq("A"))
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

    saas.join(other, JoinType.LEFT, new JoinMappingDto(other.name, "id", saas.name, "id"));
    saas.join(another, JoinType.INNER, new JoinMappingDto(another.name, "id", saas.name, "id"));

    QueryDto build = QueryBuilder2
            .from("saas")
            .leftOuterJoin("other")
            .on(other.name, "id", saas.name, "id")
            .innerJoin("another")
            .on(another.name, "id", saas.name, "id")
            .select(List.of("col1", "col2"), List.of(sum))
            .build();

    Assertions.assertThat(build).isEqualTo(q);
  }
}
