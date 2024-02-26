package io.squashql.util;

import io.squashql.query.*;
import io.squashql.query.dto.GroupColumnSetDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.TableDto;
import io.squashql.query.parameter.Parameter;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static io.squashql.query.TableField.tableField;

public class TestDatabaseQueryCreation {

  @Test
  void testNoTable() {
    QueryDto query = new QueryDto();
    query.table = new TableDto();
    Assertions.assertThatThrownBy(() -> new QueryResolver(query, Collections.emptyMap()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("table or sub-query was expected");
  }

  @Test
  void testSubQueryOfSubQuery() {
    QueryDto subSubQuery = new QueryDto();
    QueryDto subQuery = new QueryDto().table(subSubQuery);
    QueryDto queryDto = new QueryDto().table(subQuery);

    Assertions.assertThatThrownBy(() -> new QueryResolver(queryDto, Collections.emptyMap()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not supported");
  }

  @Test
  void testColumnSetInSubQuery() {
    QueryDto subQuery = new QueryDto().table("table")
            .withColumnSet(ColumnSetKey.GROUP, new GroupColumnSetDto("a", tableField("b")));
    QueryDto queryDto = new QueryDto().table(subQuery);

    Assertions.assertThatThrownBy(() -> new QueryResolver(queryDto, Collections.emptyMap()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("column sets are not expected");
  }

  @Test
  void testParametersInSubQuery() {
    QueryDto subQuery = new QueryDto().table("table");
    subQuery.withParameter("any", Mockito.mock(Parameter.class));
    QueryDto queryDto = new QueryDto().table(subQuery);

    Assertions.assertThatThrownBy(() -> new QueryResolver(queryDto, Collections.emptyMap()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("parameters are not expected");
  }

  @Test
  void testUnsupportedMeasureInSubQuery() {
    QueryDto subQuery = new QueryDto().table("table")
            .withMeasure(new ComparisonMeasureReferencePosition("alias", ComparisonMethod.DIVIDE, new AggregatedMeasure("p", "price", "sum"), Collections.emptyMap(), ColumnSetKey.GROUP));
    QueryDto queryDto = new QueryDto().table(subQuery);

    Assertions.assertThatThrownBy(() -> new QueryResolver(queryDto, Collections.emptyMap()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Only measures that can be computed by the underlying database can be used in a sub-query");
  }
}
