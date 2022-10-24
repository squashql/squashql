package me.paulbares.query.builder;

import me.paulbares.query.ColumnSet;
import me.paulbares.query.QueryBuilder;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.TableDto;
import org.junit.jupiter.api.Test;

import java.util.List;

import static me.paulbares.query.QueryBuilder.sum;

public class TestQueryBuilder2 {

  @Test
  void testSimple() {
    ColumnSet year = QueryBuilder.createPeriodColumnSet(new Period.Year("Year"));

    QueryBuilder2
            .from("saas")
            .select(List.of("col1", "col2"), List.of(year), List.of(sum("sum", "f2")));

    // Only one condition
    QueryBuilder2
            .from("saas")
            .where("f1", QueryBuilder.eq("A"))
            .select(List.of("col1", "col2"), List.of(year), List.of(sum("sum", "f2")));

    // Multiple conditions
    QueryBuilder2
            .from("saas")
            .where("f1", QueryBuilder.eq("A"))
            .where("f2", QueryBuilder.eq("B"))
            .select(List.of("col1", "col2"), List.of(year), List.of(sum("sum", "f2")));

    // FIXME test equals query dto
  }

  @Test
  void testWithSingleJoin() {
    TableDto saas = new TableDto("saas");
    TableDto other = new TableDto("other");

    QueryBuilder2
            .from("saas")
            .left_outer_join("other")
            .on(other.name, "id", saas.name, "id")
            .select(List.of("col1", "col2"), List.of(sum("sum", "f2")));

    // With two join conditions
    QueryBuilder2
            .from("saas")
            .inner_join("other")
            .on(other.name, "id", saas.name, "id")
            .on(other.name, "a", saas.name, "b")
            .select(List.of("col1", "col2"), List.of(sum("sum", "f2")));

    // With condition on the "joined" table
    QueryBuilder2
            .from("saas")
            .inner_join("other")
            .on(other.name, "id", saas.name, "id")
            .where("f1", QueryBuilder.eq("A"))
            .select(List.of("col1", "col2"), List.of(sum("sum", "f2")));

    // FIXME test equals query dto
  }

  @Test
  void testWithMultipleJoins() {
    TableDto saas = new TableDto("saas");
    TableDto other = new TableDto("other");
    TableDto another = new TableDto("another");

    QueryBuilder2
            .from("saas")
            .left_outer_join("other")
            .on(other.name, "id", saas.name, "id")
            .inner_join("another")
            .on(another.name, "id", saas.name, "id")
            .select(List.of("col1", "col2"), List.of(sum("sum", "f2")));

    // FIXME test equals query dto
  }
}
