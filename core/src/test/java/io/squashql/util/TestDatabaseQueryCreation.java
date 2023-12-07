package io.squashql.util;

import io.squashql.query.*;
import io.squashql.query.dto.BucketColumnSetDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.parameter.Parameter;
import io.squashql.type.TypedField;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.function.Function;

import static io.squashql.query.TableField.tableField;

public class TestDatabaseQueryCreation {

  private final Function<Field, TypedField> fieldSupplier = Mockito.mock(Function.class);
//todo-mde move to compile query test
//  @Test
//  void testNoTable() {
//    Assertions.assertThatThrownBy(() -> Queries.queryScopeToDatabaseQuery(QueryExecutor.createQueryScope(new QueryDto(), this.fieldSupplier), this.fieldSupplier, -1))
//            .isInstanceOf(IllegalArgumentException.class)
//            .hasMessageContaining("table or sub-query was expected");
//  }
//
//  @Test
//  void testSubQueryOfSubQuery() {
//    QueryDto subSubQuery = new QueryDto();
//    QueryDto subQuery = new QueryDto().table(subSubQuery);
//    QueryDto queryDto = new QueryDto().table(subQuery);
//
//    Assertions.assertThatThrownBy(() -> Queries.queryScopeToDatabaseQuery(QueryExecutor.createQueryScope(queryDto, this.fieldSupplier), this.fieldSupplier, -1))
//            .isInstanceOf(IllegalArgumentException.class)
//            .hasMessageContaining("not supported");
//  }
//
//  @Test
//  void testColumnSetInSubQuery() {
//    QueryDto subQuery = new QueryDto().table("table")
//            .withColumnSet(ColumnSetKey.BUCKET, new BucketColumnSetDto("a", tableField("b")));
//    QueryDto queryDto = new QueryDto().table(subQuery);
//
//    Assertions.assertThatThrownBy(() -> Queries.queryScopeToDatabaseQuery(QueryExecutor.createQueryScope(queryDto, this.fieldSupplier), this.fieldSupplier, -1))
//            .isInstanceOf(IllegalArgumentException.class)
//            .hasMessageContaining("column sets are not expected");
//  }
//
//  @Test
//  void testParametersInSubQuery() {
//    QueryDto subQuery = new QueryDto().table("table");
//    subQuery.withParameter("any", Mockito.mock(Parameter.class));
//    QueryDto queryDto = new QueryDto().table(subQuery);
//
//    Assertions.assertThatThrownBy(() -> Queries.queryScopeToDatabaseQuery(QueryExecutor.createQueryScope(queryDto, this.fieldSupplier), this.fieldSupplier, -1))
//            .isInstanceOf(IllegalArgumentException.class)
//            .hasMessageContaining("parameters are not expected");
//  }
//
//  @Test
//  void testUnsupportedMeasureInSubQuery() {
//    QueryDto subQuery = new QueryDto().table("table")
//            .withMeasure(new ComparisonMeasureReferencePosition("alias", ComparisonMethod.DIVIDE, Mockito.mock(Measure.class), Collections.emptyMap(), ColumnSetKey.BUCKET));
//    QueryDto queryDto = new QueryDto().table(subQuery);
//
//    Assertions.assertThatThrownBy(() -> Queries.queryScopeToDatabaseQuery(QueryExecutor.createQueryScope(queryDto, this.fieldSupplier), this.fieldSupplier, -1))
//            .isInstanceOf(IllegalArgumentException.class)
//            .hasMessageContaining("Only measures that can be computed by the underlying database can be used in a sub-query");
//  }
}
