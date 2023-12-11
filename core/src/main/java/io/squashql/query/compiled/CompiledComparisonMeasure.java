package io.squashql.query.compiled;

import io.squashql.query.ComparisonMeasureReferencePosition;
import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;

import java.util.List;
import java.util.Map;

public record CompiledComparisonMeasure(ComparisonMeasureReferencePosition measure, CompiledMeasure reference, Map<TypedField, String> referencePosition, CompiledPeriod period, List<TypedField> ancestors) implements CompiledMeasure {


  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    return null;
  }

  @Override
  public String alias() {
    return this.measure.alias;
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}