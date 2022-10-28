package me.paulbares.util;

import me.paulbares.query.*;
import me.paulbares.query.context.ContextValue;
import me.paulbares.query.dto.Period;
import me.paulbares.query.dto.PeriodColumnSetDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.function.Function;

public class TestDatabaseQueryCreation {

  private final Function<String, Field> fieldSupplier = Mockito.mock(Function.class);

  @Test
  void testNoTable() {
    Assertions.assertThatThrownBy(() -> Queries.queryScopeToDatabaseQuery(QueryExecutor.createQueryScope(new QueryDto(), this.fieldSupplier)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("table or sub-query was expected");
  }

  @Test
  void testSubQueryOfSubQuery() {
    QueryDto subSubQuery = new QueryDto();
    QueryDto subQuery = new QueryDto().table(subSubQuery);
    QueryDto queryDto = new QueryDto().table(subQuery);

    Assertions.assertThatThrownBy(() -> Queries.queryScopeToDatabaseQuery(QueryExecutor.createQueryScope(queryDto, this.fieldSupplier)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not supported");
  }

  @Test
  void testColumnSetInSubQuery() {
    QueryDto subQuery = new QueryDto().table("table")
            .withColumnSet(ColumnSetKey.PERIOD, new PeriodColumnSetDto(new Period.Year("year")));
    QueryDto queryDto = new QueryDto().table(subQuery);

    Assertions.assertThatThrownBy(() -> Queries.queryScopeToDatabaseQuery(QueryExecutor.createQueryScope(queryDto, this.fieldSupplier)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("column sets are not expected");
  }

  @Test
  void testContextValuesInSubQuery() {
    QueryDto subQuery = new QueryDto().table("table");
    subQuery.context("any", Mockito.mock(ContextValue.class));
    QueryDto queryDto = new QueryDto().table(subQuery);

    Assertions.assertThatThrownBy(() -> Queries.queryScopeToDatabaseQuery(QueryExecutor.createQueryScope(queryDto, this.fieldSupplier)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("context values are not expected");
  }

  @Test
  void testUnsupportedMeasureInSubQuery() {
    QueryDto subQuery = new QueryDto().table("table")
            .withMeasure(new ComparisonMeasureReferencePosition("alias", ComparisonMethod.DIVIDE, Mockito.mock(Measure.class), Collections.emptyMap()));
    QueryDto queryDto = new QueryDto().table(subQuery);

    Assertions.assertThatThrownBy(() -> Queries.queryScopeToDatabaseQuery(QueryExecutor.createQueryScope(queryDto, this.fieldSupplier)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Only AggregatedMeasure, ExpressionMeasure or BinaryOperationMeasure can be used in a sub-query");
  }
}
