package io.squashql.util;

import io.squashql.query.*;
import io.squashql.query.builder.Query;
import io.squashql.query.dto.GroupColumnSetDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.TableDto;
import io.squashql.query.field.AliasedField;
import io.squashql.query.measure.AggregatedMeasure;
import io.squashql.query.parameter.Parameter;
import io.squashql.store.Store;
import io.squashql.type.TableTypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.squashql.query.field.TableField.tableField;
import static io.squashql.query.field.TableField.tableFields;

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

  @Test
  void testUnsupportedMeasureInSubSubQuery() {
    ComparisonMeasureReferencePosition m = new ComparisonMeasureReferencePosition("alias", ComparisonMethod.DIVIDE, new AggregatedMeasure("p", "price", "sum"), Collections.emptyMap(), ColumnSetKey.GROUP);
    QueryDto subQuery2 = Query.from("table")
            .select(tableFields(List.of("a", "b")), List.of(m))
            .build();
    QueryDto subQuery1 = Query.from(subQuery2)
            .select(tableFields(List.of("a")), List.of(Functions.sum("alias2", new AliasedField("alias"))))
            .build();
    QueryDto query = Query.from(subQuery1)
            .select(List.of(), List.of(Functions.sum("alias2", new AliasedField("alias"))))
            .build();

    Map<String, Store> tables = Map.of("table", new Store("table",
            List.of(new TableTypedField("table", "a", String.class),
                    new TableTypedField("table", "b", String.class))));
    Assertions.assertThatThrownBy(() -> new QueryResolver(query, tables))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Only measures that can be computed by the underlying database can be used in a sub-query");
  }
}
